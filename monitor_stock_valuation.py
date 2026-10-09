import argparse
import logging
import math
import os
import re
import sqlite3
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Optional

import akshare as ak
import pandas as pd
import requests
import yaml


@dataclass
class SymbolRecord:
    symbol_code: str
    display_name: str


@dataclass
class IndexThermometer:
    index_code: str
    index_name: str
    close_price: Optional[float]
    pe_ttm: Optional[float]
    pe_percentile: Optional[float]
    dividend_yield: Optional[float]
    temperature: Optional[float]
    status_badge: str
    status_color: str
    status_bg: str
    guidance: str


class ConfigError(Exception):
    pass


logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    required = ["akshare", "paths", "alerts"]
    for key in required:
        if key not in config:
            raise ConfigError(f"Missing config section: {key}")

    email_cfg = config.get("email") or {}
    password_env = email_cfg.get("password_env")
    if password_env:
        env_password = os.environ.get(password_env)
        if env_password:
            email_cfg["password"] = env_password

    return config


def ensure_directories(config: dict) -> None:
    paths = config["paths"]
    for key in ["data_dir", "output_dir"]:
        Path(paths[key]).mkdir(parents=True, exist_ok=True)


def ensure_snapshot_columns(conn: sqlite3.Connection) -> None:
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(symbol_snapshot)").fetchall()
    }
    additions = {
        "dividend_yield": "ALTER TABLE symbol_snapshot ADD COLUMN dividend_yield REAL",
        "dividend_ttm": "ALTER TABLE symbol_snapshot ADD COLUMN dividend_ttm REAL",
        "dividend_updated_at": "ALTER TABLE symbol_snapshot ADD COLUMN dividend_updated_at TEXT",
        "consecutive_dividend_years": "ALTER TABLE symbol_snapshot ADD COLUMN consecutive_dividend_years INTEGER",
        "dividend_yield_10y_percentile": "ALTER TABLE symbol_snapshot ADD COLUMN dividend_yield_10y_percentile REAL",
        "dividend_yield_10y_median": "ALTER TABLE symbol_snapshot ADD COLUMN dividend_yield_10y_median REAL",
    }
    changed = False
    for column_name, sql in additions.items():
        if column_name not in columns:
            conn.execute(sql)
            changed = True
    if changed:
        conn.commit()


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbols (
            symbol_code TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            source_line TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS valuation_daily (
            symbol_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            pe_ttm REAL,
            pb REAL,
            PRIMARY KEY (symbol_code, trade_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_snapshot (
            symbol_code TEXT PRIMARY KEY,
            industry TEXT,
            current_price REAL,
            price_trade_date TEXT,
            dividend_yield REAL,
            dividend_ttm REAL,
            dividend_updated_at TEXT,
            consecutive_dividend_years INTEGER,
            dividend_yield_10y_percentile REAL,
            dividend_yield_10y_median REAL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS index_snapshot (
            index_code TEXT PRIMARY KEY,
            index_name TEXT NOT NULL,
            close_price REAL,
            pe_ttm REAL,
            pe_10y_percentile REAL,
            dividend_yield REAL,
            temperature REAL,
            eval_text TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    ensure_snapshot_columns(conn)
    conn.commit()
    return conn


def normalize_symbol_code(raw: str) -> Optional[str]:
    text = str(raw).strip().upper()
    if not text:
        return None
    match = re.search(r"(\d{6})", text)
    if not match:
        return None
    return match.group(1)


def load_stock_pool(pool_path: Path) -> list[tuple[str, Optional[str], str]]:
    rows: list[tuple[str, Optional[str], str]] = []
    with pool_path.open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line or "，" in line:
                normalized = line.replace("，", ",")
                explicit_code, display_name = [part.strip() for part in normalized.split(",", 1)]
                rows.append((display_name or explicit_code, normalize_symbol_code(explicit_code), line))
                continue

            parts = line.split(None, 1)
            if len(parts) == 2:
                explicit_code, display_name = parts[0].strip(), parts[1].strip()
                normalized_code = normalize_symbol_code(explicit_code)
                if normalized_code:
                    rows.append((display_name or explicit_code, normalized_code, line))
                    continue

            rows.append((line, None, line))
    if not rows:
        raise ConfigError(f"Stock pool is empty: {pool_path}")
    return rows


def fetch_stock_basic() -> Optional[pd.DataFrame]:
    try:
        frame = ak.stock_info_a_code_name()
    except Exception as exc:
        logger.warning("AkShare stock_info_a_code_name failed (common on overseas cloud IPs): %s", exc)
        return None

    if frame is None or frame.empty:
        return None

    rename_map = {}
    if "code" in frame.columns:
        rename_map["code"] = "symbol_code"
    elif "证券代码" in frame.columns:
        rename_map["证券代码"] = "symbol_code"

    if "name" in frame.columns:
        rename_map["name"] = "display_name"
    elif "证券简称" in frame.columns:
        rename_map["证券简称"] = "display_name"
    elif "名称" in frame.columns:
        rename_map["名称"] = "display_name"

    frame = frame.rename(columns=rename_map)
    if "symbol_code" not in frame.columns or "display_name" not in frame.columns:
        return None

    frame = frame[["symbol_code", "display_name"]].copy()
    frame["symbol_code"] = frame["symbol_code"].map(normalize_symbol_code)
    frame["display_name"] = frame["display_name"].astype(str).str.strip()
    frame = frame.dropna(subset=["symbol_code"])
    frame = frame.drop_duplicates(subset=["symbol_code", "display_name"])
    return frame


def resolve_symbols(
    pool_rows: list[tuple[str, Optional[str], str]],
    stock_basic: Optional[pd.DataFrame],
) -> tuple[list[SymbolRecord], list[str]]:
    resolved: list[SymbolRecord] = []
    issues: list[str] = []
    by_code = (
        stock_basic.drop_duplicates(subset=["symbol_code"]).set_index("symbol_code")
        if stock_basic is not None and not stock_basic.empty
        else None
    )

    for display_name, explicit_code, source_line in pool_rows:
        if explicit_code:
            matched_name = (
                str(by_code.loc[explicit_code, "display_name"])
                if by_code is not None and explicit_code in by_code.index
                else display_name
            )
            resolved.append(SymbolRecord(symbol_code=explicit_code, display_name=display_name or matched_name))
            continue

        if stock_basic is None or stock_basic.empty:
            issues.append(f"{display_name} -> stock basic list unavailable to resolve code")
            continue

        matches = stock_basic[stock_basic["display_name"] == display_name].copy()
        if matches.empty:
            issues.append(f"{display_name} -> no exact name match found")
            continue
        if len(matches) > 1:
            choices = ", ".join(matches["symbol_code"].tolist())
            issues.append(f"{display_name} -> multiple matches found: {choices}")
            continue

        row = matches.iloc[0]
        resolved.append(SymbolRecord(symbol_code=row["symbol_code"], display_name=display_name))

    return resolved, issues


def upsert_symbols(conn: sqlite3.Connection, symbols: list[SymbolRecord]) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    payload = [
        (symbol.symbol_code, symbol.display_name, f"{symbol.symbol_code},{symbol.display_name}", now)
        for symbol in symbols
    ]
    conn.executemany(
        """
        INSERT INTO symbols (symbol_code, display_name, source_line, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol_code) DO UPDATE SET
            display_name = excluded.display_name,
            source_line = excluded.source_line,
            updated_at = excluded.updated_at
        """,
        payload,
    )
    conn.commit()


def extract_date_column(frame: pd.DataFrame) -> str:
    for candidate in ["date", "日期", "trade_date"]:
        if candidate in frame.columns:
            return candidate
    raise RuntimeError(f"Unable to find date column in valuation data: {list(frame.columns)}")


def extract_value_column(frame: pd.DataFrame, indicator: str) -> str:
    candidates = ["value", indicator, "市盈率(TTM)", "市净率", "数值"]
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    non_date_columns = [column for column in frame.columns if column not in {"date", "日期", "trade_date"}]
    if len(non_date_columns) == 1:
        return non_date_columns[0]
    raise RuntimeError(f"Unable to find value column for {indicator}: {list(frame.columns)}")


def fetch_indicator_history(symbol_code: str, indicator: str) -> pd.DataFrame:
    try:
        frame = ak.stock_zh_valuation_baidu(symbol=symbol_code, indicator=indicator, period="全部")
    except Exception as exc:
        logger.exception("Failed to fetch valuation history for %s %s: %s", symbol_code, indicator, exc)
        return pd.DataFrame(columns=["symbol_code", "trade_date", "value"])
    if frame is None or frame.empty:
        logger.warning("Empty valuation history for %s %s", symbol_code, indicator)
        return pd.DataFrame(columns=["symbol_code", "trade_date", "value"])

    try:
        date_col = extract_date_column(frame)
        value_col = extract_value_column(frame, indicator)
        normalized = frame[[date_col, value_col]].copy()
        normalized.columns = ["trade_date", "value"]
        normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], errors="coerce").dt.strftime("%Y%m%d")
        normalized["value"] = pd.to_numeric(normalized["value"], errors="coerce")
        normalized["symbol_code"] = symbol_code
        normalized = normalized.dropna(subset=["trade_date"])
        normalized = normalized.drop_duplicates(subset=["symbol_code", "trade_date"])
        return normalized[["symbol_code", "trade_date", "value"]]
    except Exception as exc:
        logger.exception("Failed to normalize valuation history for %s %s: %s", symbol_code, indicator, exc)
        return pd.DataFrame(columns=["symbol_code", "trade_date", "value"])


def fetch_spot_prices_map() -> dict[str, float]:
    try:
        frame = ak.stock_zh_a_spot()
    except Exception:
        return {}
    if frame is None or frame.empty:
        return {}

    columns = list(frame.columns)
    code_col = columns[0] if len(columns) >= 1 else None
    price_col = columns[2] if len(columns) >= 3 else None
    if code_col is None or price_col is None:
        return {}

    mapping: dict[str, float] = {}
    for _, row in frame.iterrows():
        code = normalize_symbol_code(row.get(code_col))
        price = safe_float(row.get(price_col))
        if code and price is not None and price > 0:
            mapping[code] = price
    return mapping


def fetch_current_price(symbol_code: str, spot_price_map: Optional[dict[str, float]] = None) -> tuple[Optional[float], Optional[str]]:
    if spot_price_map:
        cached_spot_price = safe_float(spot_price_map.get(symbol_code))
        if cached_spot_price is not None and cached_spot_price > 0:
            return cached_spot_price, date.today().strftime("%Y%m%d")

    try:
        frame = ak.stock_bid_ask_em(symbol=symbol_code)
    except Exception:
        frame = None

    if isinstance(frame, pd.DataFrame) and not frame.empty and {"item", "value"}.issubset(frame.columns):
        quote_map = dict(zip(frame["item"].astype(str), frame["value"]))
        for key in ["最新", "最新价"]:
            price = safe_float(quote_map.get(key))
            if price is not None:
                return price, date.today().strftime("%Y%m%d")

    start_date = (date.today() - timedelta(days=15)).strftime("%Y%m%d")
    end_date = date.today().strftime("%Y%m%d")
    try:
        hist = ak.stock_zh_a_hist(symbol=symbol_code, period="daily", start_date=start_date, end_date=end_date, adjust="")
    except Exception:
        return None, None
    if hist is None or hist.empty or "收盘" not in hist.columns:
        return None, None

    price = safe_float(hist.iloc[-1]["收盘"])
    trade_date = pd.to_datetime(hist.iloc[-1].get("日期"), errors="coerce")
    if pd.isna(trade_date):
        return price, None
    return price, trade_date.strftime("%Y%m%d")


def fetch_industry(symbol_code: str) -> Optional[str]:
    try:
        frame = ak.stock_individual_info_em(symbol=symbol_code)
    except Exception:
        return None
    if frame is None or frame.empty or not {"item", "value"}.issubset(frame.columns):
        return None

    info_map = dict(zip(frame["item"].astype(str), frame["value"]))
    for key in ["行业", "所属行业"]:
        industry = str(info_map.get(key, "")).strip()
        if industry:
            return industry
    return None


def suggest_primary_indicator(display_name: str, industry: Optional[str]) -> str:
    text = f"{display_name} {industry or ''}"

    dividend_pb_keywords = [
        "银行",
        "保险",
        "煤",
        "煤炭",
        "石油",
        "海油",
        "石化",
        "矿",
        "矿业",
        "有色",
        "铝",
        "铜",
        "黄金",
        "航运",
        "海运",
        "港口",
        "海控",
        "建筑",
        "交建",
        "工程",
        "铁路",
        "基建",
        "神华",
        "人寿",
        "太保",
    ]
    if any(keyword in text for keyword in dividend_pb_keywords):
        if "保险" in text or "人寿" in text or "太保" in text:
            return "PB为主，股息率补充，EV辅助"
        return "PB为主，股息率补充，PE辅助"

    dividend_pe_keywords = ["移动", "通信", "电信", "公用事业", "电力", "高速公路", "长江电力"]
    if any(keyword in text for keyword in dividend_pe_keywords):
        return "PE为主，股息率补充，PB辅助"

    return "PB为主，股息率补充，PE辅助"


def primary_indicator_percentile(primary_indicator: str, pe_percentile: Optional[float], pb_percentile: Optional[float]) -> Optional[float]:
    if str(primary_indicator).startswith("PB"):
        return pb_percentile
    return pe_percentile


def load_symbol_snapshot(
    conn: sqlite3.Connection,
    symbol_code: str,
) -> tuple[
    Optional[str],
    Optional[float],
    Optional[str],
    Optional[float],
    Optional[float],
    Optional[str],
    Optional[int],
    Optional[float],
    Optional[float],
]:
    row = conn.execute(
        """
        SELECT industry, current_price, price_trade_date, dividend_yield, dividend_ttm, dividend_updated_at, consecutive_dividend_years, dividend_yield_10y_percentile, dividend_yield_10y_median
        FROM symbol_snapshot
        WHERE symbol_code = ?
        """,
        (symbol_code,),
    ).fetchone()
    if row is None:
        return None, None, None, None, None, None, None, None, None
    (
        industry,
        current_price,
        price_trade_date,
        dividend_yield,
        dividend_ttm,
        dividend_updated_at,
        consecutive_years,
        div_pct,
        div_median,
    ) = row
    return (
        industry,
        safe_float(current_price),
        price_trade_date,
        safe_float(dividend_yield),
        safe_float(dividend_ttm),
        dividend_updated_at,
        int(consecutive_years) if consecutive_years is not None else None,
        safe_float(div_pct),
        safe_float(div_median),
    )


def upsert_symbol_snapshot(
    conn: sqlite3.Connection,
    symbol_code: str,
    industry: Optional[str],
    current_price: Optional[float],
    price_trade_date: Optional[str],
    dividend_yield: Optional[float],
    dividend_ttm: Optional[float],
    dividend_updated_at: Optional[str],
    consecutive_dividend_years: Optional[int],
    dividend_yield_10y_percentile: Optional[float] = None,
    dividend_yield_10y_median: Optional[float] = None,
) -> None:
    (
        existing_industry,
        existing_price,
        existing_price_trade_date,
        existing_dividend_yield,
        existing_dividend_ttm,
        existing_dividend_updated_at,
        existing_consecutive_years,
        existing_div_pct,
        existing_div_median,
    ) = load_symbol_snapshot(conn, symbol_code)
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO symbol_snapshot (
            symbol_code, industry, current_price, price_trade_date,
            dividend_yield, dividend_ttm, dividend_updated_at, consecutive_dividend_years,
            dividend_yield_10y_percentile, dividend_yield_10y_median, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol_code) DO UPDATE SET
            industry = excluded.industry,
            current_price = excluded.current_price,
            price_trade_date = excluded.price_trade_date,
            dividend_yield = excluded.dividend_yield,
            dividend_ttm = excluded.dividend_ttm,
            dividend_updated_at = excluded.dividend_updated_at,
            consecutive_dividend_years = excluded.consecutive_dividend_years,
            dividend_yield_10y_percentile = excluded.dividend_yield_10y_percentile,
            dividend_yield_10y_median = excluded.dividend_yield_10y_median,
            updated_at = excluded.updated_at
        """,
        (
            symbol_code,
            industry if industry else existing_industry,
            current_price if current_price is not None else existing_price,
            price_trade_date if price_trade_date else existing_price_trade_date,
            dividend_yield if dividend_yield is not None else existing_dividend_yield,
            dividend_ttm if dividend_ttm is not None else existing_dividend_ttm,
            dividend_updated_at if dividend_updated_at else existing_dividend_updated_at,
            consecutive_dividend_years if consecutive_dividend_years is not None else existing_consecutive_years,
            dividend_yield_10y_percentile if dividend_yield_10y_percentile is not None else existing_div_pct,
            dividend_yield_10y_median if dividend_yield_10y_median is not None else existing_div_median,
            now,
        ),
    )
    conn.commit()


def sync_symbol_history(conn: sqlite3.Connection, symbol_code: str) -> int:
    pe_frame = fetch_indicator_history(symbol_code, "市盈率(TTM)").rename(columns={"value": "pe_ttm"})
    pb_frame = fetch_indicator_history(symbol_code, "市净率").rename(columns={"value": "pb"})

    merged = pd.merge(pe_frame, pb_frame, on=["symbol_code", "trade_date"], how="outer")
    if merged.empty:
        return 0

    merged = merged.sort_values("trade_date").reset_index(drop=True)
    conn.execute("DELETE FROM valuation_daily WHERE symbol_code = ?", (symbol_code,))
    conn.executemany(
        """
        INSERT INTO valuation_daily (symbol_code, trade_date, pe_ttm, pb)
        VALUES (?, ?, ?, ?)
        """,
        merged[["symbol_code", "trade_date", "pe_ttm", "pb"]].itertuples(index=False, name=None),
    )
    conn.commit()
    return len(merged)


def load_symbol_history(conn: sqlite3.Connection, symbol_code: str, start_date: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT symbol_code, trade_date, pe_ttm, pb
        FROM valuation_daily
        WHERE symbol_code = ? AND trade_date >= ?
        ORDER BY trade_date ASC
        """,
        conn,
        params=(symbol_code, start_date),
    )


def percentile_of_current(series: pd.Series) -> Optional[float]:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric[numeric.notna() & (numeric > 0)]
    if valid.empty:
        return None
    current = float(valid.iloc[-1])
    return float((valid <= current).mean())


def safe_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def normalize_cn_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.copy()


def first_matching_column(columns: list[str], keywords: list[str]) -> Optional[str]:
    for column in columns:
        text = str(column)
        if all(keyword in text for keyword in keywords):
            return column
    return None


def fetch_dividend_metrics(symbol_code: str, current_price: Optional[float]) -> tuple[Optional[float], Optional[float], Optional[str], Optional[int]]:
    try:
        frame = ak.stock_history_dividend_detail(symbol=symbol_code, indicator="分红")
    except Exception:
        return None, None, None, None
    if frame is None or frame.empty:
        return None, None, None, None

    frame = normalize_cn_columns(frame)
    columns = list(frame.columns)
    announcement_col = first_matching_column(columns, ["公告"])
    dividend_col = first_matching_column(columns, ["派息"])
    status_col = first_matching_column(columns, ["进度"])

    if announcement_col is None and len(columns) >= 1:
        announcement_col = columns[0]
    if dividend_col is None and len(columns) >= 4:
        dividend_col = columns[3]
    if status_col is None and len(columns) >= 5:
        status_col = columns[4]
    if announcement_col is None or dividend_col is None:
        return None, None, None, None

    working = frame.copy()
    if status_col is not None:
        status_text = working[status_col].astype(str)
        implemented = working[status_text.str.contains("实施", na=False)].copy()
        if not implemented.empty:
            working = implemented

    working[announcement_col] = pd.to_datetime(working[announcement_col], errors="coerce")
    working[dividend_col] = pd.to_numeric(working[dividend_col], errors="coerce")
    working = working.dropna(subset=[announcement_col, dividend_col])
    if working.empty:
        return None, None, None, None

    working = working.sort_values(announcement_col).reset_index(drop=True)
    latest_date_value = working.iloc[-1][announcement_col]
    latest_date = pd.Timestamp(latest_date_value).strftime("%Y%m%d")

    # 计算连续分红年数
    dividend_years = set(
        working[working[dividend_col] > 0][announcement_col].dt.year.dropna().astype(int).tolist()
    )
    current_year = date.today().year
    consecutive_years = 0
    check_year = current_year if current_year in dividend_years else (current_year - 1)
    while check_year in dividend_years:
        consecutive_years += 1
        check_year -= 1

    cutoff_date = pd.Timestamp(date.today() - timedelta(days=365))
    trailing = working[working[announcement_col] >= cutoff_date].copy()
    if trailing.empty:
        trailing = working.tail(1).copy()

    dividend_ttm = safe_float(trailing[dividend_col].sum() / 10.0)
    if dividend_ttm is None or dividend_ttm <= 0 or current_price is None or current_price <= 0:
        return None, dividend_ttm, latest_date, consecutive_years

    dividend_yield = dividend_ttm / current_price
    return dividend_yield, dividend_ttm, latest_date, consecutive_years


def dividend_passes_threshold(dividend_yield: Optional[float], dividend_threshold: float) -> Optional[bool]:
    if dividend_yield is None:
        return None
    return dividend_yield >= dividend_threshold


def fetch_dividend_yield_percentile(
    symbol_code: str,
    current_price: Optional[float],
    dividend_ttm: Optional[float],
) -> tuple[Optional[float], Optional[float]]:
    """
    通过东财估值走势与历史分红记录计算个股近10年股息率历史分位数及中位数
    """
    if not current_price or current_price <= 0 or not dividend_ttm or dividend_ttm <= 0:
        return None, None

    try:
        div_frame = ak.stock_history_dividend_detail(symbol=symbol_code, indicator="分红")
        if div_frame is None or div_frame.empty:
            return None, None

        ann_col = first_matching_column(list(div_frame.columns), ["公告"]) or div_frame.columns[0]
        payout_col = first_matching_column(list(div_frame.columns), ["派息"]) or div_frame.columns[3]
        status_col = first_matching_column(list(div_frame.columns), ["进度"])

        working = div_frame.copy()
        if status_col:
            working = working[working[status_col].astype(str).str.contains("实施", na=False)]
        working[ann_col] = pd.to_datetime(working[ann_col], errors="coerce")
        working[payout_col] = pd.to_numeric(working[payout_col], errors="coerce") / 10.0
        working = working.dropna(subset=[ann_col, payout_col]).sort_values(ann_col)
        if working.empty:
            return None, None

        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "sortColumns": "TRADE_DATE",
            "sortTypes": "1",
            "pageSize": "2500",
            "pageNumber": "1",
            "reportName": "RPT_VALUEANALYSIS_DET",
            "columns": "TRADE_DATE,CLOSE_PRICE",
            "source": "WEB",
            "client": "WEB",
            "filter": f'(SECURITY_CODE="{symbol_code}")',
        }
        resp = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        data_json = resp.json()
        items = (data_json.get("result") or {}).get("data") or []
        if not items:
            return None, None

        price_df = pd.DataFrame(items)
        price_df["TRADE_DATE"] = pd.to_datetime(price_df["TRADE_DATE"], errors="coerce")
        price_df["CLOSE_PRICE"] = pd.to_numeric(price_df["CLOSE_PRICE"], errors="coerce")
        price_df = price_df.dropna(subset=["TRADE_DATE", "CLOSE_PRICE"])

        daily_yields = []
        for _, row in price_df.iterrows():
            t_date = row["TRADE_DATE"]
            price = row["CLOSE_PRICE"]
            if price and price > 0:
                dps = working[(working[ann_col] <= t_date) & (working[ann_col] >= t_date - pd.Timedelta(days=365))][payout_col].sum()
                if dps > 0:
                    daily_yields.append(dps / price)

        if not daily_yields:
            return None, None

        series = pd.Series(daily_yields)
        curr_yield = dividend_ttm / current_price
        pct = float((series <= curr_yield).mean())
        median_val = float(series.median())
        return pct, median_val
    except Exception as exc:
        logger.warning("Failed to calculate dividend yield percentile for %s: %s", symbol_code, exc)
        return None, None


def fetch_index_thermometer(conn: sqlite3.Connection, index_code: str, index_name: str) -> IndexThermometer:
    """
    抓取中证指数(如中证红利000922、沪深300 000300)并计算估值温度
    """
    now = datetime.now().isoformat(timespec="seconds")
    cached_row = conn.execute(
        """
        SELECT close_price, pe_ttm, pe_10y_percentile, dividend_yield, temperature, eval_text, updated_at
        FROM index_snapshot WHERE index_code = ?
        """,
        (index_code,),
    ).fetchone()

    cached_close = safe_float(cached_row[0]) if cached_row else None
    cached_pe = safe_float(cached_row[1]) if cached_row else None
    cached_pct = safe_float(cached_row[2]) if cached_row else None
    cached_div = safe_float(cached_row[3]) if cached_row else None
    cached_temp = safe_float(cached_row[4]) if cached_row else None

    close_price = None
    pe_ttm = None
    pe_pct = None
    div_yield = None

    try:
        start_date = (date.today() - timedelta(days=3650 + 10)).strftime("%Y%m%d")
        end_date = date.today().strftime("%Y%m%d")
        hist = ak.stock_zh_index_hist_csindex(symbol=index_code, start_date=start_date, end_date=end_date)
        if hist is not None and not hist.empty and "滚动市盈率" in hist.columns:
            pe_clean = pd.to_numeric(hist["滚动市盈率"], errors="coerce").dropna()
            if not pe_clean.empty:
                pe_ttm = float(pe_clean.iloc[-1])
                pe_pct = float((pe_clean <= pe_ttm).mean())
            if "收盘" in hist.columns:
                close_series = pd.to_numeric(hist["收盘"], errors="coerce").dropna()
                if not close_series.empty:
                    close_price = float(close_series.iloc[-1])

        val_df = ak.stock_zh_index_value_csindex(symbol=index_code)
        if val_df is not None and not val_df.empty:
            for div_col in ["股息率2", "股息率1"]:
                if div_col in val_df.columns:
                    val = safe_float(val_df.iloc[-1][div_col])
                    if val is not None and val > 0:
                        div_yield = val / 100.0
                        break
    except Exception as exc:
        logger.warning("Failed to fetch live CSIndex data for %s (%s): %s", index_code, index_name, exc)

    final_close = close_price if close_price is not None else cached_close
    final_pe = pe_ttm if pe_ttm is not None else cached_pe
    final_pct = pe_pct if pe_pct is not None else cached_pct
    final_div = div_yield if div_yield is not None else cached_div
    temperature = (final_pct * 100.0) if final_pct is not None else cached_temp

    if temperature is not None:
        if temperature < 30.0:
            status_badge = "❄️ 极度严寒"
            status_color = "#0969da"
            status_bg = "#ddf4ff"
            guidance = "估值处于历史大底（黄金击球区），安全边际极高，建议积极定投加仓"
        elif temperature < 50.0:
            status_badge = "🍃 清凉舒适"
            status_color = "#1a7f37"
            status_bg = "#dafbe1"
            guidance = "估值偏低舒适，性价比较高，适合分步稳健建仓"
        elif temperature < 70.0:
            status_badge = "⛅ 温和均衡"
            status_color = "#9a6700"
            status_bg = "#fff8c5"
            guidance = "估值合理适中，不宜追高，精选深度折价个股配置"
        elif temperature < 85.0:
            status_badge = "☀️ 偏暖微热"
            status_color = "#bf8700"
            status_bg = "#fff8c5"
            guidance = "估值分位偏高，建议严格执行低吸策略，逢高适度防守"
        else:
            status_badge = "🌋 盛夏过热"
            status_color = "#cf222e"
            status_bg = "#ffebe9"
            guidance = "估值处于历史顶部过热区，建议以防守和分批止盈为主"
    else:
        status_badge = "🌡️ 观测中"
        status_color = "#586069"
        status_bg = "#f6f8fa"
        guidance = "数据同步中，保持纪律性建仓"

    eval_text = f"{status_badge} - {guidance}"
    conn.execute(
        """
        INSERT INTO index_snapshot (index_code, index_name, close_price, pe_ttm, pe_10y_percentile, dividend_yield, temperature, eval_text, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(index_code) DO UPDATE SET
            close_price = excluded.close_price,
            pe_ttm = excluded.pe_ttm,
            pe_10y_percentile = excluded.pe_10y_percentile,
            dividend_yield = excluded.dividend_yield,
            temperature = excluded.temperature,
            eval_text = excluded.eval_text,
            updated_at = excluded.updated_at
        """,
        (index_code, index_name, final_close, final_pe, final_pct, final_div, temperature, eval_text, now),
    )
    conn.commit()

    return IndexThermometer(
        index_code=index_code,
        index_name=index_name,
        close_price=final_close,
        pe_ttm=final_pe,
        pe_percentile=final_pct,
        dividend_yield=final_div,
        temperature=temperature,
        status_badge=status_badge,
        status_color=status_color,
        status_bg=status_bg,
        guidance=guidance,
    )


def fetch_market_thermometers(conn: sqlite3.Connection) -> list[IndexThermometer]:
    indices = [
        ("000922", "中证红利"),
        ("000300", "沪深300"),
    ]
    results = []
    for code, name in indices:
        thermometer = fetch_index_thermometer(conn, code, name)
        results.append(thermometer)
    return results


def build_valuation_reason(
    primary_indicator: str,
    primary_percentile: Optional[float],
    threshold: float,
    dividend_yield: Optional[float],
    dividend_threshold: float,
    strategy_mode: str,
    is_watch_candidate: bool,
) -> str:
    metric_name = "PB" if str(primary_indicator).startswith("PB") else "PE"
    percentile_text = format_percent(primary_percentile) if primary_percentile is not None else "无数据"
    dividend_text = format_percent(dividend_yield) if dividend_yield is not None else "无数据"
    threshold_text = format_percent(threshold)
    dividend_threshold_text = format_percent(dividend_threshold)

    if primary_percentile is None:
        return f"{metric_name}近10年分位缺失，暂无法判断低估"
    if primary_percentile > threshold:
        return f"{metric_name}近10年分位 {percentile_text}，高于阈值 {threshold_text}"
    if strategy_mode == "strict":
        if dividend_yield is None:
            return f"{metric_name}近10年分位 {percentile_text} 已达标，但股息率暂无法计算（通常是当前价格抓取失败）"
        if dividend_yield < dividend_threshold:
            return f"{metric_name}近10年分位 {percentile_text} 已达标，但股息率 {dividend_text} 低于 {dividend_threshold_text}"
    if is_watch_candidate:
        return f"{metric_name}近10年分位 {percentile_text}，股息率 {dividend_text}，进入重点观察区间"
    return f"{metric_name}近10年分位 {percentile_text} 已达标，股息率 {dividend_text} 作为一般观察项"


def build_conclusion(is_watch_candidate: bool, strategy_mode: str) -> str:
    if is_watch_candidate:
        return "进入重点观察区间"
    if strategy_mode == "strict":
        return "未进入重点观察区间（严格条件）"
    return "暂未进入重点观察区间"


def evaluate_dividend_quality(
    consecutive_years: Optional[int],
    payout_ratio: Optional[float],
    div_yield_10y_percentile: Optional[float] = None,
    div_yield_10y_median: Optional[float] = None,
) -> tuple[str, str, str, str, bool]:
    """
    评估分红质量与股息陷阱风险
    返回: (continuity_desc, payout_desc, div_hist_desc, quality_label, is_trap)
    """
    if consecutive_years is not None and consecutive_years > 0:
        continuity_desc = f"连续 {consecutive_years} 年分红"
    else:
        continuity_desc = "分红记录偏短/暂无"

    if payout_ratio is not None and payout_ratio > 0:
        pct_str = f"{payout_ratio:.1%}"
        if payout_ratio > 1.0:
            payout_desc = f"{pct_str} (⚠️ 掏空利润)"
        elif payout_ratio > 0.8:
            payout_desc = f"{pct_str} (⚡ 缓冲偏小)"
        elif payout_ratio >= 0.3:
            payout_desc = f"{pct_str} (💎 黄金区间)"
        else:
            payout_desc = f"{pct_str} (🌱 留存充裕)"
    else:
        payout_desc = "待测算"

    if div_yield_10y_percentile is not None:
        div_pct_str = f"{div_yield_10y_percentile:.1%}"
        if div_yield_10y_percentile >= 0.8:
            div_hist_desc = f"自身10年分位 {div_pct_str} (👑 极具吸引力)"
        elif div_yield_10y_percentile >= 0.6:
            div_hist_desc = f"自身10年分位 {div_pct_str} (🌟 偏高吸引力)"
        elif div_yield_10y_percentile >= 0.4:
            div_hist_desc = f"自身10年分位 {div_pct_str} (⛅ 历史适中)"
        else:
            div_hist_desc = f"自身10年分位 {div_pct_str} (🌱 历史偏低位)"
    else:
        div_hist_desc = "自身历史分位待测"

    is_trap = bool(payout_ratio is not None and payout_ratio > 1.0)

    if is_trap:
        quality_label = "🚨 警惕股息陷阱（支付率超 100%，利润难以为继）"
    elif consecutive_years is not None and consecutive_years >= 10 and payout_ratio is not None and 0.3 <= payout_ratio <= 0.8:
        quality_label = "🛡️ 优质高息 · 黄金分红（排雷满分）"
    elif consecutive_years is not None and consecutive_years >= 5:
        quality_label = "🛡️ 持续回馈 · 排雷正常"
    else:
        quality_label = "🛡️ 排雷正常"

    return continuity_desc, payout_desc, div_hist_desc, quality_label, is_trap


def build_summary(
    conn: sqlite3.Connection,
    symbols: list[SymbolRecord],
    lookback_years: int,
    threshold: float,
    dividend_threshold: float,
    strategy_mode: str,
) -> pd.DataFrame:
    start_date = (date.today() - timedelta(days=lookback_years * 365 + 10)).strftime("%Y%m%d")
    rows = []
    spot_price_map = fetch_spot_prices_map()

    for symbol in symbols:
        (
            cached_industry,
            cached_price,
            cached_price_trade_date,
            cached_dividend_yield,
            cached_dividend_ttm,
            cached_dividend_updated_at,
            cached_consecutive_years,
            cached_div_10y_pct,
            cached_div_10y_median,
        ) = load_symbol_snapshot(conn, symbol.symbol_code)
        industry = fetch_industry(symbol.symbol_code) or cached_industry
        primary_indicator = suggest_primary_indicator(symbol.display_name, industry)
        current_price, price_trade_date = fetch_current_price(symbol.symbol_code, spot_price_map)
        if current_price is None:
            current_price = cached_price
        if not price_trade_date:
            price_trade_date = cached_price_trade_date

        dividend_yield, dividend_ttm, dividend_updated_at, consecutive_years = fetch_dividend_metrics(symbol.symbol_code, current_price)
        if dividend_yield is None:
            dividend_yield = cached_dividend_yield
        if dividend_ttm is None:
            dividend_ttm = cached_dividend_ttm
        if not dividend_updated_at:
            dividend_updated_at = cached_dividend_updated_at
        if consecutive_years is None:
            consecutive_years = cached_consecutive_years

        div_10y_pct = cached_div_10y_pct
        div_10y_median = cached_div_10y_median
        if div_10y_pct is None and current_price and dividend_ttm:
            div_10y_pct, div_10y_median = fetch_dividend_yield_percentile(symbol.symbol_code, current_price, dividend_ttm)

        upsert_symbol_snapshot(
            conn,
            symbol.symbol_code,
            industry,
            current_price,
            price_trade_date,
            dividend_yield,
            dividend_ttm,
            dividend_updated_at,
            consecutive_years,
            div_10y_pct,
            div_10y_median,
        )
        history = load_symbol_history(conn, symbol.symbol_code, start_date)
        if history.empty:
            primary_percentile = None
            dividend_ok = dividend_passes_threshold(dividend_yield, dividend_threshold)
            is_watch_candidate = False
            continuity_desc, payout_desc, div_hist_desc, quality_label, is_trap = evaluate_dividend_quality(
                consecutive_years, None, div_10y_pct, div_10y_median
            )
            rows.append(
                {
                    "display_name": symbol.display_name,
                    "symbol_code": symbol.symbol_code,
                    "industry": industry,
                    "primary_indicator": primary_indicator,
                    "latest_trade_date": price_trade_date,
                    "current_price": current_price,
                    "pe_ttm": None,
                    "pe_ttm_10y_percentile": None,
                    "pb": None,
                    "pb_10y_percentile": None,
                    "dividend_yield": dividend_yield,
                    "dividend_ttm": dividend_ttm,
                    "dividend_updated_at": dividend_updated_at,
                    "dividend_pass": dividend_ok,
                    "consecutive_dividend_years": consecutive_years,
                    "dividend_payout_ratio": None,
                    "dividend_yield_10y_percentile": div_10y_pct,
                    "dividend_yield_10y_median": div_10y_median,
                    "dividend_continuity_desc": continuity_desc,
                    "dividend_payout_desc": payout_desc,
                    "dividend_hist_desc": div_hist_desc,
                    "dividend_quality_label": quality_label,
                    "is_dividend_trap": is_trap,
                    "主指标近10年分位": primary_percentile,
                    "is_watch_candidate": is_watch_candidate,
                    "低估判断依据": build_valuation_reason(
                        primary_indicator,
                        primary_percentile,
                        threshold,
                        dividend_yield,
                        dividend_threshold,
                        strategy_mode,
                        is_watch_candidate,
                    ),
                    "综合低估结论": build_conclusion(is_watch_candidate, strategy_mode),
                    "sample_size": 0,
                    "target_entry_price": None,
                    "target_price_div": None,
                    "target_price_val": None,
                    "price_gap_pct": None,
                    "bottleneck_reason": "历史估值数据缺失",
                }
            )
            continue

        history = history.sort_values("trade_date").reset_index(drop=True)
        latest_trade_date = history.iloc[-1]["trade_date"]
        pe_series = pd.to_numeric(history["pe_ttm"], errors="coerce")
        pb_series = pd.to_numeric(history["pb"], errors="coerce")
        pe_clean = pe_series[pe_series.notna() & (pe_series > 0)]
        pb_clean = pb_series[pb_series.notna() & (pb_series > 0)]
        latest_pe = safe_float(pe_clean.iloc[-1]) if not pe_clean.empty else None
        latest_pb = safe_float(pb_clean.iloc[-1]) if not pb_clean.empty else None
        pe_percentile = percentile_of_current(pe_series)
        pb_percentile = percentile_of_current(pb_series)
        primary_percentile = primary_indicator_percentile(primary_indicator, pe_percentile, pb_percentile)
        dividend_ok = dividend_passes_threshold(dividend_yield, dividend_threshold)
        primary_ok = primary_percentile is not None and primary_percentile <= threshold
        if strategy_mode == "strict":
            is_watch_candidate = bool(primary_ok and dividend_ok is True)
        else:
            is_watch_candidate = bool(primary_ok)

        # 股利支付率与分红排雷评估
        payout_ratio = None
        if dividend_yield is not None and latest_pe is not None and latest_pe > 0:
            payout_ratio = float(dividend_yield * latest_pe)

        continuity_desc, payout_desc, div_hist_desc, quality_label, is_trap = evaluate_dividend_quality(
            consecutive_years, payout_ratio, div_10y_pct, div_10y_median
        )

        # 目标进场价格测算
        target_price_div = None
        if dividend_ttm and dividend_ttm > 0 and dividend_threshold > 0:
            target_price_div = float(dividend_ttm / dividend_threshold)

        target_price_val = None
        if current_price and current_price > 0:
            if str(primary_indicator).startswith("PB") and not pb_clean.empty and latest_pb and latest_pb > 0:
                pb_target_val = float(pb_clean.quantile(threshold))
                target_price_val = float(current_price * (pb_target_val / latest_pb))
            elif not pe_clean.empty and latest_pe and latest_pe > 0:
                pe_target_val = float(pe_clean.quantile(threshold))
                target_price_val = float(current_price * (pe_target_val / latest_pe))

        target_entry_price = None
        if strategy_mode == "strict":
            if target_price_div is not None and target_price_val is not None:
                target_entry_price = min(target_price_div, target_price_val)
            elif target_price_div is not None:
                target_entry_price = target_price_div
            elif target_price_val is not None:
                target_entry_price = target_price_val
        else:
            target_entry_price = target_price_val or target_price_div

        price_gap_pct = None
        if target_entry_price is not None and current_price and current_price > 0:
            price_gap_pct = float((target_entry_price - current_price) / current_price)

        bottleneck_reason = ""
        if not is_watch_candidate:
            if primary_ok and not dividend_ok:
                bottleneck_reason = f"估值分位已达标({primary_percentile:.1%})，主要等股息率达标(当前{dividend_yield:.2% if dividend_yield else '无'})"
            elif dividend_ok and not primary_ok:
                pct_str = f"{primary_percentile:.1%}" if primary_percentile is not None else "缺失"
                bottleneck_reason = f"股息率达标({dividend_yield:.2%})，主要等估值分位回调至20%以内(当前{pct_str})"
            else:
                pct_str = f"{primary_percentile:.1%}" if primary_percentile is not None else "缺失"
                div_str = f"{dividend_yield:.2%}" if dividend_yield is not None else "无"
                bottleneck_reason = f"估值分位({pct_str})与股息率({div_str})均未达标，需进一步回调"

        rows.append(
            {
                "display_name": symbol.display_name,
                "symbol_code": symbol.symbol_code,
                "industry": industry,
                "primary_indicator": primary_indicator,
                "latest_trade_date": latest_trade_date,
                "current_price": current_price,
                "pe_ttm": latest_pe,
                "pe_ttm_10y_percentile": pe_percentile,
                "pb": latest_pb,
                "pb_10y_percentile": pb_percentile,
                "dividend_yield": dividend_yield,
                "dividend_ttm": dividend_ttm,
                "dividend_updated_at": dividend_updated_at,
                "dividend_pass": dividend_ok,
                "consecutive_dividend_years": consecutive_years,
                "dividend_payout_ratio": payout_ratio,
                "dividend_yield_10y_percentile": div_10y_pct,
                "dividend_yield_10y_median": div_10y_median,
                "dividend_continuity_desc": continuity_desc,
                "dividend_payout_desc": payout_desc,
                "dividend_hist_desc": div_hist_desc,
                "dividend_quality_label": quality_label,
                "is_dividend_trap": is_trap,
                "主指标近10年分位": primary_percentile,
                "is_watch_candidate": is_watch_candidate,
                "低估判断依据": build_valuation_reason(
                    primary_indicator,
                    primary_percentile,
                    threshold,
                    dividend_yield,
                    dividend_threshold,
                    strategy_mode,
                    is_watch_candidate,
                ),
                "综合低估结论": build_conclusion(is_watch_candidate, strategy_mode),
                "sample_size": int(max(len(pe_clean), len(pb_clean))),
                "target_entry_price": target_entry_price,
                "target_price_div": target_price_div,
                "target_price_val": target_price_val,
                "price_gap_pct": price_gap_pct,
                "bottleneck_reason": bottleneck_reason,
            }
        )

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    summary["pe_below_20pct"] = summary["pe_ttm_10y_percentile"].notna() & (summary["pe_ttm_10y_percentile"] <= threshold)
    summary["pb_below_20pct"] = summary["pb_10y_percentile"].notna() & (summary["pb_10y_percentile"] <= threshold)
    summary["min_percentile"] = summary[["pe_ttm_10y_percentile", "pb_10y_percentile"]].min(axis=1, skipna=True)
    summary = summary.sort_values(
        by=["is_watch_candidate", "主指标近10年分位", "dividend_yield", "display_name"],
        ascending=[False, True, False, True],
    )
    return summary


def format_optional_bool(value: Optional[bool]) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return "是" if bool(value) else "否"


def format_percent(value: Optional[float]) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return f"{value:.2%}"


def format_metric(value: Optional[float]) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return f"{value:.2f}"


def save_reports(summary: pd.DataFrame, output_dir: Path, threshold: float, dividend_threshold: float, strategy_mode: str) -> tuple[Path, Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / "latest_valuation.csv"
    html_path = output_dir / "latest_valuation.html"
    alerts_path = output_dir / "alerts.csv"

    csv_df = summary.rename(
        columns={
            "display_name": "股票名称",
            "symbol_code": "股票代码",
            "industry": "所属行业",
            "primary_indicator": "建议主看指标",
            "latest_trade_date": "最新交易日",
            "current_price": "当前价格",
            "pe_ttm": "当前滚动市盈率",
            "pe_ttm_10y_percentile": "滚动市盈率近10年分位",
            "pb": "当前市净率",
            "pb_10y_percentile": "市净率近10年分位",
            "dividend_yield": "当前股息率",
            "dividend_ttm": "近12个月每股分红",
            "dividend_updated_at": "股息数据更新日",
            "dividend_pass": "股息率是否达标",
            "consecutive_dividend_years": "连续分红年数",
            "dividend_payout_ratio": "股利支付率",
            "dividend_yield_10y_percentile": "股息率近10年分位",
            "dividend_yield_10y_median": "股息率近10年中位数",
            "dividend_quality_label": "分红排雷评估",
            "sample_size": "有效样本数",
            "pe_below_20pct": "滚动市盈率是否低估",
            "pb_below_20pct": "市净率是否低估",
            "主指标近10年分位": "建议主看指标近10年分位",
            "is_watch_candidate": "是否进入重点观察区间",
            "min_percentile": "市盈率市净率较低分位",
            "低估判断依据": "低估判断依据",
            "综合低估结论": "综合低估结论",
            "target_entry_price": "目标买入价",
            "price_gap_pct": "距目标价所需跌幅",
            "bottleneck_reason": "观察瓶颈说明",
        }
    )

    for col in ["所属行业", "建议主看指标", "低估判断依据", "综合低估结论", "股息数据更新日", "分红排雷评估"]:
        if col in csv_df.columns:
            csv_df[col] = csv_df[col].fillna("")
    for col in ["滚动市盈率是否低估", "市净率是否低估", "是否进入重点观察区间", "股息率是否达标"]:
        if col in csv_df.columns:
            csv_df[col] = csv_df[col].map(format_optional_bool)

    try:
        csv_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        csv_path = output_dir / f"latest_valuation_{timestamp}.csv"
        csv_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    display_df = csv_df.copy()
    for col in ["滚动市盈率近10年分位", "市净率近10年分位", "建议主看指标近10年分位", "市盈率市净率较低分位", "当前股息率", "距目标价所需跌幅", "股利支付率", "股息率近10年分位", "股息率近10年中位数"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].map(format_percent)
    if "连续分红年数" in display_df.columns:
        display_df["连续分红年数"] = display_df["连续分红年数"].map(lambda v: f"{int(v)}年" if pd.notna(v) and v is not None and str(v) != "" else "")
    for col in ["当前价格", "当前滚动市盈率", "当前市净率", "近12个月每股分红", "目标买入价"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].map(format_metric)

    html_table = display_df.to_html(index=False, escape=False)
    html = f"""
    <html>
      <head>
        <meta charset=\"utf-8\" />
        <style>
          body {{ font-family: Arial, sans-serif; padding: 16px; }}
          table {{ border-collapse: collapse; width: 100%; }}
          th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
          th {{ background: #f5f5f5; }}
        </style>
      </head>
      <body>
        <h2>股票池月度估值观察</h2>
        <p>主规则：PB为主、股息率补充、PE辅助</p>
        <p>主指标阈值：近 10 年分位 ≤ {threshold:.0%}；股息率阈值：≥ {dividend_threshold:.1%}；模式：{strategy_mode}</p>
        {html_table}
      </body>
    </html>
    """
    try:
        html_path.write_text(html, encoding="utf-8")
    except PermissionError:
        html_path = output_dir / f"latest_valuation_{timestamp}.html"
        html_path.write_text(html, encoding="utf-8")

    alerts = csv_df[csv_df["是否进入重点观察区间"] == "是"].copy()
    try:
        alerts.to_csv(alerts_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        alerts_path = output_dir / f"alerts_{timestamp}.csv"
        alerts.to_csv(alerts_path, index=False, encoding="utf-8-sig")
    return csv_path, html_path, alerts_path


def get_entry_analysis(
    code: str,
    name: str,
    pb: Optional[float],
    pb_pct: Optional[float],
    pe: Optional[float],
    pe_pct: Optional[float],
    div_yield: Optional[float],
) -> dict:
    specific_notes = {
        "000651": {
            "tier_name": "🥇 第一梯队（优先进场）",
            "tier_badge": "优先进场",
            "tier_color": "#28a745",
            "badge_bg": "#e6ffed",
            "badge_border": "#b4e6c0",
            "tag": "超高股息 · 现金奶牛",
            "highlight": "股息率逼近 8%（全池最高），历史极低估值，账面现金极充沛，分红意愿极强。",
            "strategy": "随时可底仓配置，逢市场回调加大定投力度。",
            "rank": 1,
        },
        "600036": {
            "tier_name": "🥇 第一梯队（优先进场）",
            "tier_badge": "优先进场",
            "tier_color": "#28a745",
            "badge_bg": "#e6ffed",
            "badge_border": "#b4e6c0",
            "tag": "零售之王 · 攻守兼备",
            "highlight": "破净估值处于历史底部（分位 <15%），资产质量极优，兼顾近 5% 稳健股息与估值修复弹性。",
            "strategy": "破净区间是中长线建仓黄金期，建议逢低分步买入。",
            "rank": 2,
        },
        "601668": {
            "tier_name": "🥇 第一梯队（优先进场）",
            "tier_badge": "优先进场",
            "tier_color": "#28a745",
            "badge_bg": "#e6ffed",
            "badge_border": "#b4e6c0",
            "tag": "极限折价 · 央企基石",
            "highlight": "PB 仅 0.36（十年分位 1.09%），安全垫极厚；股息率超 6%，海外与基建订单充沛。",
            "strategy": "波动极小，跌无可跌，极佳的稳健吃息底仓标的。",
            "rank": 3,
        },
        "601006": {
            "tier_name": "🥈 第二梯队（观察配置）",
            "tier_badge": "观察配置",
            "tier_color": "#d97706",
            "badge_bg": "#fff8c5",
            "badge_border": "#f1e05a",
            "tag": "类固收 · 铁路生命线",
            "highlight": "煤运垄断通道，PB 处于历史低位，股息约 4.7%；但当前受运量影响 PE 偏高。",
            "strategy": "可作为高息“永续债”看待，适合在 4.5~4.7 元附近逢低分批网格挂单。",
            "rank": 4,
        },
        "600019": {
            "tier_name": "🥈 第二梯队（观察配置）",
            "tier_badge": "观察配置",
            "tier_color": "#d97706",
            "badge_bg": "#fff8c5",
            "badge_border": "#f1e05a",
            "tag": "周期博弈 · 行业龙头",
            "highlight": "国内钢铁绝对霸主，处于行业利润周期低谷，PB 深度折价，股息超 5%。",
            "strategy": "适合看好顺周期反转的逆向资金小仓位潜伏；纯稳健吃息首选第一梯队。",
            "rank": 5,
        },
        "600941": {
            "tier_name": "🥇 第一梯队（优先进场）",
            "tier_badge": "优先进场",
            "tier_color": "#28a745",
            "badge_bg": "#e6ffed",
            "badge_border": "#b4e6c0",
            "tag": "通信霸主 · 稳健现金流",
            "highlight": "护城河极深，利润与现金流极度稳定，高分红承诺。",
            "strategy": "长线收息首选，回踩重要均线分批吸纳。",
            "rank": 1,
        },
        "600900": {
            "tier_name": "🥇 第一梯队（优先进场）",
            "tier_badge": "优先进场",
            "tier_color": "#28a745",
            "badge_bg": "#e6ffed",
            "badge_border": "#b4e6c0",
            "tag": "水电印钞机 · 类永续债",
            "highlight": "长江干流水电绝对垄断，来水即现金，现金分红极其慷慨。",
            "strategy": "只要回调进入低估区间即是无脑长线配置机会。",
            "rank": 1,
        },
    }

    if code in specific_notes:
        return specific_notes[code]

    dy = div_yield or 0.0
    p_pct = pb_pct if pb_pct is not None else 1.0
    if dy >= 0.06 or (dy >= 0.048 and p_pct <= 0.10):
        return {
            "tier_name": "🥇 第一梯队（优先进场）",
            "tier_badge": "优先进场",
            "tier_color": "#28a745",
            "badge_bg": "#e6ffed",
            "badge_border": "#b4e6c0",
            "tag": "深度价值 · 高确定性",
            "highlight": f"历史估值分位处于历史底部，且股息率达 {dy:.2%}，安全边际极高。",
            "strategy": "建议底仓配置，结合市场节奏逢低吸纳。",
            "rank": 2,
        }
    else:
        return {
            "tier_name": "🥈 第二梯队（观察配置）",
            "tier_badge": "观察配置",
            "tier_color": "#d97706",
            "badge_bg": "#fff8c5",
            "badge_border": "#f1e05a",
            "tag": "稳健观察 · 逢低配置",
            "highlight": f"估值分位达标，股息率 {dy:.2%} 满足门槛，但需留意行业周期波动。",
            "strategy": "建议保持观察，等待更低价格或分步网格轻仓介入。",
            "rank": 6,
        }


def build_email_message(
    config: dict,
    summary: pd.DataFrame,
    candidate_count: int,
    thermometers: Optional[list[IndexThermometer]] = None,
) -> EmailMessage:
    email_cfg = config.get("email") or {}
    strategy_cfg = config.get("strategy") or {}
    alerts_cfg = config.get("alerts") or {}
    required_keys = ["smtp_host", "smtp_port", "username", "password", "from_addr", "to_addrs"]
    missing = [key for key in required_keys if not email_cfg.get(key)]
    if missing:
        raise ConfigError(f"Missing email config keys: {', '.join(missing)}")

    report_date = date.today().strftime("%Y-%m-%d")
    dividend_threshold = float(strategy_cfg.get("dividend_yield_threshold", 0.045))
    percentile_threshold = float(alerts_cfg.get("percentile_threshold", 0.20))
    strategy_mode = str(strategy_cfg.get("mode", "strict"))

    candidates = summary[summary["is_watch_candidate"] == True].copy() if not summary.empty else pd.DataFrame()

    text_lines = [
        "【股票池高股息估值观察与进场建议】",
        f"日期：{report_date}",
        f"筛选规则：主指标近 10 年分位 ≤ {percentile_threshold:.0%}，近 12 个月股息率 ≥ {dividend_threshold:.1%}",
        f"重点观察标的数：{candidate_count} 只",
        "-" * 48,
    ]

    if thermometers:
        text_lines.append("🌡️【大盘与红利整体水温计】")
        for t in thermometers:
            div_s = f" ｜ 股息率: {t.dividend_yield:.2%}" if t.dividend_yield else ""
            text_lines.append(f"   • {t.index_name}({t.index_code}): 水温 {t.temperature:.1f}℃ 【{t.status_badge}】 (PE: {t.pe_ttm:.2f}, 10年分位: {t.pe_percentile:.1%}{div_s})")
            text_lines.append(f"     指导建议: {t.guidance}")
        text_lines.append("-" * 48)

    thermometers_html = ""
    if thermometers:
        cards_html = []
        for t in thermometers:
            div_val_str = f"{t.dividend_yield:.2%}" if t.dividend_yield is not None else "—"
            cards_html.append(f"""
            <div style="flex: 1; min-width: 240px; background: #ffffff; border: 1px solid #e1e4e8; border-radius: 6px; padding: 12px 14px; box-shadow: 0 1px 2px rgba(0,0,0,0.03);">
              <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                <span style="font-size: 15px; font-weight: bold; color: #1a1a1a;">{t.index_name} <span style="font-size: 12px; color: #586069; font-weight: normal;">({t.index_code})</span></span>
                <span style="background: {t.status_bg}; color: {t.status_color}; border: 1px solid {t.status_color}33; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 600;">{t.status_badge}</span>
              </div>
              <div style="display: flex; align-items: baseline; gap: 8px; margin-bottom: 4px;">
                <span style="font-size: 22px; font-weight: bold; color: {t.status_color};">{t.temperature:.1f}℃</span>
                <span style="font-size: 13px; color: #586069;">PE: <b>{t.pe_ttm:.2f}</b> (近10年分位 <b>{t.pe_percentile:.1%}</b>)</span>
              </div>
              <div style="font-size: 13px; color: #444; margin-bottom: 6px;">
                指数股息率：<b style="color: #28a745;">{div_val_str}</b>
              </div>
              <div style="font-size: 12px; color: #586069; line-height: 1.5; border-top: 1px dashed #e1e4e8; padding-top: 6px;">
                🧭 <b>配置建议</b>：{t.guidance}
              </div>
            </div>
            """)
        thermometers_html = f"""
        <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 14px; margin-bottom: 16px;">
          <div style="font-size: 14px; font-weight: bold; color: #1e293b; margin-bottom: 10px; display: flex; align-items: center; justify-content: space-between;">
            <span>🌡️ 大盘与红利整体水温计</span>
            <span style="font-size: 12px; color: #64748b; font-weight: normal;">水温越低估值越便宜，胜率越高</span>
          </div>
          <div style="display: flex; gap: 10px; flex-wrap: wrap;">
            {''.join(cards_html)}
          </div>
        </div>
        """

    html_cards = []
    if candidates.empty:
        text_lines.append("本次暂无标的进入重点观察区间。")
        html_cards.append("<p style='color: #666; font-size: 15px;'>本次暂无标的进入重点观察区间。</p>")
    else:
        # 对标的按照梯队优先级排序
        enriched_list = []
        for _, row in candidates.iterrows():
            code = str(row.get("symbol_code", ""))
            name = str(row.get("display_name", ""))
            pb_val = safe_float(row.get("pb"))
            pb_pct_val = safe_float(row.get("pb_10y_percentile"))
            pe_val = safe_float(row.get("pe_ttm"))
            pe_pct_val = safe_float(row.get("pe_ttm_10y_percentile"))
            div_val = safe_float(row.get("dividend_yield"))
            entry_info = get_entry_analysis(code, name, pb_val, pb_pct_val, pe_val, pe_pct_val, div_val)
            enriched_list.append((entry_info["rank"], row, entry_info))

        enriched_list.sort(key=lambda x: x[0])

        for idx, (_, row, entry_info) in enumerate(enriched_list, 1):
            name = str(row.get("display_name", ""))
            code = str(row.get("symbol_code", ""))
            price_val = safe_float(row.get("current_price"))
            price = f"{price_val:.2f} 元" if price_val is not None else "无"
            div_val = safe_float(row.get("dividend_yield"))
            div_yield = f"{div_val:.2%}" if div_val is not None else "无"
            div_ttm_val = safe_float(row.get("dividend_ttm"))
            div_ttm = f"{div_ttm_val:.2f} 元" if div_ttm_val is not None else "无"
            div_hist_desc = str(row.get("dividend_hist_desc", ""))

            pb_val = safe_float(row.get("pb"))
            pb = f"{pb_val:.2f}" if pb_val is not None else "无"
            pb_pct_val = safe_float(row.get("pb_10y_percentile"))
            pb_pct = f"{pb_pct_val:.2%}" if pb_pct_val is not None else "无"

            pe_val = safe_float(row.get("pe_ttm"))
            pe = f"{pe_val:.2f}" if pe_val is not None else "无"
            pe_pct_val = safe_float(row.get("pe_ttm_10y_percentile"))
            pe_pct = f"{pe_pct_val:.2%}" if pe_pct_val is not None else "无"

            primary_ind = str(row.get("primary_indicator", "PB"))
            target_val = safe_float(row.get("target_entry_price"))
            target_str = f"{target_val:.2f} 元" if target_val is not None else "当前价"

            # 分红排雷与质量评估
            consec_val = row.get("consecutive_dividend_years")
            consec_str = f"连续 {int(consec_val)} 年分红" if consec_val is not None and pd.notna(consec_val) and int(consec_val) > 0 else "分红记录偏短"
            payout_desc = str(row.get("dividend_payout_desc", "待测算"))
            quality_label = str(row.get("dividend_quality_label", "🛡️ 排雷正常"))
            is_trap = bool(row.get("is_dividend_trap", False))

            trap_alert_text = "\n   • ⚠️【股息陷阱警报】：支付率超 100%，净利润无法覆盖分红，谨防本金亏损！" if is_trap else ""
            trap_alert_html = """
              <div style="background: #ffeef0; color: #cb2431; border: 1px solid #fdaeb7; padding: 6px 10px; border-radius: 4px; font-size: 13px; font-weight: 600; margin-bottom: 8px;">
                ⚠️ 股息陷阱警报：该标的股利支付率超过 100%，净利润无法覆盖分红，需谨防分红不可持续及本金损失风险！
              </div>
            """ if is_trap else ""

            text_lines.append(f"{idx}. {name} ({code}) 【{entry_info['tier_badge']}】")
            text_lines.append(f"   • 标签：{entry_info['tag']}")
            text_lines.append(f"   • 当前价格：{price} ｜ 目标买入价：≤ {target_str}（现价已进入击球区 ✅）")
            if primary_ind.startswith("PB"):
                text_lines.append(f"   • 市净率 PB：{pb}（近10年分位: {pb_pct}）")
            else:
                text_lines.append(f"   • 市盈率 PE：{pe}（近10年分位: {pe_pct}）")
            text_lines.append(f"   • 当前股息率：{div_yield}（{div_hist_desc} ｜ 近12个月每股分红: {div_ttm}）")
            text_lines.append(f"   • 分红排雷：{quality_label}（{consec_str} ｜ 支付率: {payout_desc}）{trap_alert_text}")
            text_lines.append(f"   • 核心看点：{entry_info['highlight']}")
            text_lines.append(f"   • 进场建议：{entry_info['strategy']}")
            text_lines.append("")

            html_cards.append(f"""
            <div style="background: #ffffff; border: 1px solid #e1e4e8; border-radius: 8px; padding: 16px; margin-bottom: 14px; box-shadow: 0 1px 3px rgba(0,0,0,0.04);">
              <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #f0f0f0; padding-bottom: 8px; margin-bottom: 10px;">
                <span style="font-size: 17px; font-weight: bold; color: #1a1a1a;">
                  {idx}. {name} <span style="font-size: 13px; color: #586069; font-weight: normal;">({code})</span>
                </span>
                <span style="background: {entry_info['badge_bg']}; color: {entry_info['tier_color']}; border: 1px solid {entry_info['badge_border']}; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 600;">
                  {entry_info['tier_badge']}
                </span>
              </div>
              {trap_alert_html}
              <div style="margin-bottom: 8px; font-size: 13px; color: #0366d6; font-weight: 500;">
                🏷️ <b>策略标签</b>：{entry_info['tag']}
              </div>
              <table style="width: 100%; border-collapse: collapse; font-size: 14px; line-height: 1.8; color: #333333;">
                <tr><td style="width: 100px; color: #666; padding: 2px 0;">当前价格：</td><td><b style="color: #d73a49; font-size: 15px;">{price}</b></td></tr>
                <tr><td style="color: #666; padding: 2px 0;">🎯 目标买入价：</td><td><b style="color: #0366d6;">≤ {target_str}</b> <span style="color: #28a745; font-size: 12px; font-weight: bold;">(已进入击球区 ✅)</span></td></tr>
                <tr><td style="color: #666; padding: 2px 0;">市净率 PB：</td><td><b>{pb}</b>（近 10 年分位：<b style="color: #0366d6;">{pb_pct}</b>）</td></tr>
                <tr><td style="color: #666; padding: 2px 0;">滚动市盈率 PE：</td><td><b>{pe}</b>（近 10 年分位：{pe_pct}）</td></tr>
                <tr><td style="color: #666; padding: 2px 0;">当前股息率：</td><td><b style="color: #28a745; font-size: 15px;">{div_yield}</b> <span style="color: #0366d6; font-size: 12px; font-weight: 500;">({div_hist_desc})</span> <span style="color: #586069; font-size: 12px;">（近 12 个月分红：{div_ttm}）</span></td></tr>
                <tr><td style="color: #666; padding: 2px 0;">🛡️ 分红排雷：</td><td><b style="color: {'#cb2431' if is_trap else '#28a745'};">{quality_label}</b> <span style="color: #586069; font-size: 12px;">（{consec_str} ｜ 支付率：{payout_desc}）</span></td></tr>
              </table>
              <div style="margin-top: 8px; padding: 8px 10px; background: #f6f8fa; border-radius: 4px; font-size: 13px; line-height: 1.6; color: #444;">
                <div style="margin-bottom: 4px;">🌟 <b>核心看点</b>：{entry_info['highlight']}</div>
                <div>📌 <b>进场建议</b>：<span style="color: #b05000; font-weight: 500;">{entry_info['strategy']}</span></div>
              </div>
            </div>
            """)

        text_lines.append("=" * 48)
        text_lines.append("💡 实操进场策略指南：")
        text_lines.append("1. 第一梯队（优先进场）：确定性与安全垫极高，适合作为核心底仓分批买入。")
        text_lines.append("2. 第二梯队（观察配置）：受行业周期波动影响稍大，适合作为类固收或周期博弈品种逢低网格挂单。")
        text_lines.append("3. 建仓节奏：建议采取「底仓 3~4 成 + 逢跌分批网格」策略，拒绝追高。")

    # 构建未达标标的的“备战清单”与目标进场价
    pending = summary[summary["is_watch_candidate"] == False].copy() if not summary.empty else pd.DataFrame()
    pending_html_section = ""
    if not pending.empty:
        if "price_gap_pct" in pending.columns:
            pending = pending.sort_values(by="price_gap_pct", ascending=False, na_position="last")

        text_lines.append("")
        text_lines.append("-" * 48)
        text_lines.append("🎯【潜在标的备战清单（挂单与买入点测算）】")
        text_lines.append("（按距离目标价由近及远排列，可在券商 App 提前设置条件单）")
        text_lines.append("")

        pending_rows = []
        for p_idx, (_, p_row) in enumerate(pending.iterrows(), 1):
            p_name = str(p_row.get("display_name", ""))
            p_code = str(p_row.get("symbol_code", ""))
            p_price = safe_float(p_row.get("current_price"))
            p_target = safe_float(p_row.get("target_entry_price"))
            p_gap = safe_float(p_row.get("price_gap_pct"))
            p_bottle = str(p_row.get("bottleneck_reason", ""))

            p_price_str = f"{p_price:.2f} 元" if p_price is not None else "无"
            p_target_str = f"≤ {p_target:.2f} 元" if p_target is not None else "待测算"

            p_consec_val = p_row.get("consecutive_dividend_years")
            p_consec_str = f"连续 {int(p_consec_val)} 年" if p_consec_val is not None and pd.notna(p_consec_val) and int(p_consec_val) > 0 else "记录偏短"
            p_payout_val = safe_float(p_row.get("dividend_payout_ratio"))
            p_payout_str = format_percent(p_payout_val) if p_payout_val is not None else "待测"
            p_payout_desc = str(p_row.get("dividend_payout_desc", "待测算"))
            p_div_hist = str(p_row.get("dividend_hist_desc", ""))

            if p_gap is not None:
                p_gap_str = f"{p_gap:.1%}"
                if p_gap >= -0.06:
                    p_badge = "🔔 即将达标"
                    p_color = "#b05000"
                    p_bg = "#fff8c5"
                elif p_gap >= -0.15:
                    p_badge = "⏳ 密切关注"
                    p_color = "#0366d6"
                    p_bg = "#f1f8ff"
                else:
                    p_badge = "🧘 耐心等待"
                    p_color = "#586069"
                    p_bg = "#f6f8fa"
            else:
                p_gap_str = "无"
                p_badge = "观察中"
                p_color = "#586069"
                p_bg = "#f6f8fa"

            text_lines.append(f"{p_idx}. {p_name} ({p_code}) 【{p_badge}】")
            text_lines.append(f"   • 当前现价：{p_price_str} ｜ 目标买入价：{p_target_str}（还需 {p_gap_str}）")
            text_lines.append(f"   • 分红质量：{p_consec_str}分红 ｜ 支付率: {p_payout_desc} ｜ {p_div_hist}")
            text_lines.append(f"   • 观察瓶颈：{p_bottle}")
            text_lines.append("")

            pending_rows.append(f"""
            <tr style="border-bottom: 1px solid #f0f0f0; font-size: 13px;">
              <td style="padding: 9px 6px; font-weight: bold; color: #1a1a1a;">
                {p_name} <span style="font-size: 11px; color: #666; font-weight: normal;">({p_code})</span>
              </td>
              <td style="padding: 9px 6px; color: #444;">{p_price_str}</td>
              <td style="padding: 9px 6px; font-weight: bold; color: #0366d6;">{p_target_str}</td>
              <td style="padding: 9px 6px;">
                <span style="background: {p_bg}; color: {p_color}; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: bold;">
                  还需 {p_gap_str}
                </span>
              </td>
              <td style="padding: 9px 6px; color: #444; font-size: 12px;">
                <span>{p_consec_str}</span><br/>
                <span style="color: #666; font-size: 11px;">支付率 {p_payout_str}</span><br/>
                <span style="color: #0366d6; font-size: 11px;">{p_div_hist}</span>
              </td>
              <td style="padding: 9px 6px; color: #586069; font-size: 12px;">{p_bottle}</td>
            </tr>
            """)

        pending_html_section = f"""
        <div style="margin-top: 24px; padding-top: 16px; border-top: 2px dashed #e1e4e8;">
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
            <h3 style="margin: 0; font-size: 16px; color: #1a1a1a;">🎯 潜在标的备战清单（挂单与买入点测算）</h3>
            <span style="font-size: 12px; color: #586069;">按距离目标价由近及远排列</span>
          </div>
          <div style="background: #f6f8fa; border: 1px solid #e1e4e8; border-radius: 6px; padding: 8px 12px; margin-bottom: 12px; font-size: 12px; color: #586069;">
            💡 提示：以下标的当前尚未达标，已为你倒算出触发买入信号的<b>精确击球价格</b>，可在券商 App 提前设置条件单挂单等跌。
          </div>
          <div style="overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
              <thead>
                <tr style="background: #f1f3f5; color: #495057; text-align: left; font-size: 12px;">
                  <th style="padding: 8px 6px;">标的名称</th>
                  <th style="padding: 8px 6px;">当前现价</th>
                  <th style="padding: 8px 6px;">目标买入价</th>
                  <th style="padding: 8px 6px;">距目标价</th>
                  <th style="padding: 8px 6px;">分红质量与历史分位</th>
                  <th style="padding: 8px 6px;">主要观察瓶颈</th>
                </tr>
              </thead>
              <tbody>
                {''.join(pending_rows)}
              </tbody>
            </table>
          </div>
        </div>
        """

    text_body = "\n".join(text_lines)
    html_body = f"""
    <html>
      <head><meta charset="utf-8"/></head>
      <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f6f8fa; padding: 20px 10px; color: #24292e;">
        <div style="max-width: 600px; margin: 0 auto;">
          <div style="background: #24292e; color: #ffffff; padding: 16px 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0; font-size: 19px; letter-spacing: 0.5px;">📈 股票池高股息估值观察与进场建议</h2>
            <div style="font-size: 13px; color: #959da5; margin-top: 6px;">日期：{report_date} ｜ 模式：{strategy_mode} ｜ 重点标的：{candidate_count} 只</div>
          </div>
          <div style="background: #ffffff; border: 1px solid #e1e4e8; border-top: none; border-radius: 0 0 8px 8px; padding: 18px;">
            <div style="background: #f1f8ff; border-left: 4px solid #0366d6; padding: 8px 12px; font-size: 13px; color: #0366d6; margin-bottom: 16px; border-radius: 0 4px 4px 0;">
              筛选规则：主指标近 10 年分位 ≤ {percentile_threshold:.0%}，且近 12 个月股息率 ≥ {dividend_threshold:.1%}
            </div>
            {thermometers_html}
            {''.join(html_cards)}
            <div style="background: #fdf6ec; border-left: 4px solid #e6a23c; padding: 12px 14px; margin-top: 20px; border-radius: 0 4px 4px 0; font-size: 13px; line-height: 1.8; color: #5c3c00;">
              <b>💡 实操进场策略指南：</b><br/>
              • <b>第一梯队（优先进场）</b>：现金流与分红确定性极高，具备坚实安全垫，适合作为核心底仓分批买入。<br/>
              • <b>第二梯队（观察配置）</b>：受宏观或行业周期影响稍大，适合作为类固收或周期博弈品种，建议逢低网格挂单。<br/>
              • <b>建仓节奏</b>：建议采取「底仓 3~4 成 + 逢跌分批网格」策略，拒绝追高，跌出来的才是真机会。<br/>
              • <b>挂单备战</b>：参考下方【备战清单】的目标价在券商设置“到价提醒”或“网格买入条件单”。
            </div>
            {pending_html_section}
          </div>
        </div>
      </body>
    </html>
    """

    message = EmailMessage()
    message["Subject"] = f"【股票估值与进场建议】{report_date} 共 {candidate_count} 只标的进入观察区间"
    message["From"] = email_cfg["from_addr"]
    message["To"] = ", ".join(email_cfg["to_addrs"])
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")
    return message


def send_email(
    config: dict,
    summary: pd.DataFrame,
    candidate_count: int,
    thermometers: Optional[list[IndexThermometer]] = None,
) -> None:
    message = build_email_message(config, summary, candidate_count, thermometers=thermometers)
    email_cfg = config["email"]

    if email_cfg.get("use_ssl", True):
        with smtplib.SMTP_SSL(email_cfg["smtp_host"], int(email_cfg["smtp_port"])) as smtp:
            smtp.login(email_cfg["username"], email_cfg["password"])
            smtp.send_message(message)
    else:
        with smtplib.SMTP(email_cfg["smtp_host"], int(email_cfg["smtp_port"])) as smtp:
            smtp.starttls()
            smtp.login(email_cfg["username"], email_cfg["password"])
            smtp.send_message(message)


def main() -> None:
    import sys
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Monitor 10-year PE/PB percentiles for stocks in stocks.txt")
    parser.add_argument("--config", default="monitor_config.yaml", help="Path to YAML config")
    parser.add_argument("--pool", default="stocks.txt", help="Path to stock pool file")
    parser.add_argument("--skip-email", action="store_true", help="Generate reports without sending email")
    args = parser.parse_args()

    config_path = Path(args.config)
    pool_path = Path(args.pool)
    config = load_config(config_path)
    ensure_directories(config)

    data_dir = Path(config["paths"]["data_dir"])
    output_dir = Path(config["paths"]["output_dir"])
    db_path = data_dir / "valuation_cache.sqlite3"
    threshold = float(config["alerts"].get("percentile_threshold", 0.2))
    lookback_years = int(config["alerts"].get("lookback_years", 10))
    strategy_cfg = config.get("strategy") or {}
    dividend_threshold = float(strategy_cfg.get("dividend_yield_threshold", 0.045))
    strategy_mode = str(strategy_cfg.get("mode", "strict")).strip().lower() or "strict"
    if strategy_mode not in {"strict", "loose"}:
        raise ConfigError(f"Unsupported strategy.mode: {strategy_mode}")

    conn = connect_db(db_path)
    try:
        pool_rows = load_stock_pool(pool_path)
        stock_basic = fetch_stock_basic()
        symbols, issues = resolve_symbols(pool_rows, stock_basic)
        if issues:
            issues_text = "\n".join(f"- {issue}" for issue in issues)
            raise RuntimeError(f"Stock pool has unresolved entries:\n{issues_text}")
        if not symbols:
            raise RuntimeError("No valid symbols resolved from stock pool")

        upsert_symbols(conn, symbols)
        for symbol in symbols:
            row_count = sync_symbol_history(conn, symbol.symbol_code)
            if row_count == 0:
                logger.warning("No valuation history saved for %s", symbol.symbol_code)

        summary = build_summary(conn, symbols, lookback_years, threshold, dividend_threshold, strategy_mode)
        csv_path, html_path, alerts_path = save_reports(summary, output_dir, threshold, dividend_threshold, strategy_mode)
        thermometers = fetch_market_thermometers(conn)

        email_enabled = bool((config.get("email") or {}).get("enabled", False))
        candidate_count = int(summary["is_watch_candidate"].sum()) if not summary.empty else 0
        should_send = email_enabled and (not args.skip_email)
        email_error = None
        if should_send:
            try:
                send_email(config, summary, candidate_count, thermometers=thermometers)
            except Exception as exc:
                email_error = exc

        print(f"Saved CSV: {csv_path}")
        print(f"Saved HTML: {html_path}")
        print(f"Saved alerts: {alerts_path}")
        print(f"Strategy mode: {strategy_mode}")
        print(f"Dividend threshold: {dividend_threshold:.2%}")
        print(f"Candidates: {candidate_count}")
        if thermometers:
            print("Market Thermometers:")
            for t in thermometers:
                div_info = f", 股息率 {t.dividend_yield:.2%}" if t.dividend_yield else ""
                print(f"  - {t.index_name} ({t.index_code}): {t.temperature:.1f}℃ [{t.status_badge}] (PE {t.pe_ttm:.2f}, 分位 {t.pe_percentile:.1%}{div_info})")
        if should_send and email_error is None:
            print(f"Email alerts sent for this run with {candidate_count} candidates")
        elif should_send:
            print(f"Email alerts failed: {email_error}")
            raise RuntimeError(f"Email alerts failed: {email_error}")
        elif not email_enabled:
            print("Email alerts skipped because email is disabled")
        else:
            print("Email alerts skipped because --skip-email was provided")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
