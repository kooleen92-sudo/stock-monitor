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
import yaml


@dataclass
class SymbolRecord:
    symbol_code: str
    display_name: str


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


def fetch_stock_basic() -> pd.DataFrame:
    frame = ak.stock_info_a_code_name()
    if frame is None or frame.empty:
        raise RuntimeError("AkShare stock_info_a_code_name returned no data")

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
        raise RuntimeError(f"Unexpected columns from stock_info_a_code_name: {list(frame.columns)}")

    frame = frame[["symbol_code", "display_name"]].copy()
    frame["symbol_code"] = frame["symbol_code"].map(normalize_symbol_code)
    frame["display_name"] = frame["display_name"].astype(str).str.strip()
    frame = frame.dropna(subset=["symbol_code"])
    frame = frame.drop_duplicates(subset=["symbol_code", "display_name"])
    return frame


def resolve_symbols(
    pool_rows: list[tuple[str, Optional[str], str]],
    stock_basic: pd.DataFrame,
) -> tuple[list[SymbolRecord], list[str]]:
    resolved: list[SymbolRecord] = []
    issues: list[str] = []
    by_code = stock_basic.drop_duplicates(subset=["symbol_code"]).set_index("symbol_code")

    for display_name, explicit_code, source_line in pool_rows:
        if explicit_code:
            if explicit_code not in by_code.index:
                issues.append(f"{source_line} -> code not found in A-share list")
                continue
            matched_name = str(by_code.loc[explicit_code, "display_name"])
            resolved.append(SymbolRecord(symbol_code=explicit_code, display_name=display_name or matched_name))
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
) -> tuple[Optional[str], Optional[float], Optional[str], Optional[float], Optional[float], Optional[str]]:
    row = conn.execute(
        """
        SELECT industry, current_price, price_trade_date, dividend_yield, dividend_ttm, dividend_updated_at
        FROM symbol_snapshot
        WHERE symbol_code = ?
        """,
        (symbol_code,),
    ).fetchone()
    if row is None:
        return None, None, None, None, None, None
    industry, current_price, price_trade_date, dividend_yield, dividend_ttm, dividend_updated_at = row
    return (
        industry,
        safe_float(current_price),
        price_trade_date,
        safe_float(dividend_yield),
        safe_float(dividend_ttm),
        dividend_updated_at,
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
) -> None:
    (
        existing_industry,
        existing_price,
        existing_price_trade_date,
        existing_dividend_yield,
        existing_dividend_ttm,
        existing_dividend_updated_at,
    ) = load_symbol_snapshot(conn, symbol_code)
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO symbol_snapshot (
            symbol_code, industry, current_price, price_trade_date,
            dividend_yield, dividend_ttm, dividend_updated_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol_code) DO UPDATE SET
            industry = excluded.industry,
            current_price = excluded.current_price,
            price_trade_date = excluded.price_trade_date,
            dividend_yield = excluded.dividend_yield,
            dividend_ttm = excluded.dividend_ttm,
            dividend_updated_at = excluded.dividend_updated_at,
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
    if numeric.empty:
        return None
    current = numeric.iloc[-1]
    if pd.isna(current) or current <= 0:
        return None
    valid = numeric[numeric.notna() & (numeric > 0)]
    if valid.empty:
        return None
    return float((valid <= float(current)).mean())


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


def fetch_dividend_metrics(symbol_code: str, current_price: Optional[float]) -> tuple[Optional[float], Optional[float], Optional[str]]:
    try:
        frame = ak.stock_history_dividend_detail(symbol=symbol_code, indicator="分红")
    except Exception:
        return None, None, None
    if frame is None or frame.empty:
        return None, None, None

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
        return None, None, None

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
        return None, None, None

    working = working.sort_values(announcement_col).reset_index(drop=True)
    latest_date_value = working.iloc[-1][announcement_col]
    latest_date = pd.Timestamp(latest_date_value).strftime("%Y%m%d")

    cutoff_date = pd.Timestamp(date.today() - timedelta(days=365))
    trailing = working[working[announcement_col] >= cutoff_date].copy()
    if trailing.empty:
        trailing = working.tail(1).copy()

    dividend_ttm = safe_float(trailing[dividend_col].sum() / 10.0)
    if dividend_ttm is None or dividend_ttm <= 0 or current_price is None or current_price <= 0:
        return None, dividend_ttm, latest_date

    dividend_yield = dividend_ttm / current_price
    return dividend_yield, dividend_ttm, latest_date


def dividend_passes_threshold(dividend_yield: Optional[float], dividend_threshold: float) -> Optional[bool]:
    if dividend_yield is None:
        return None
    return dividend_yield >= dividend_threshold


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
        ) = load_symbol_snapshot(conn, symbol.symbol_code)
        industry = fetch_industry(symbol.symbol_code) or cached_industry
        primary_indicator = suggest_primary_indicator(symbol.display_name, industry)
        current_price, price_trade_date = fetch_current_price(symbol.symbol_code, spot_price_map)
        if current_price is None:
            current_price = cached_price
        if not price_trade_date:
            price_trade_date = cached_price_trade_date

        dividend_yield, dividend_ttm, dividend_updated_at = fetch_dividend_metrics(symbol.symbol_code, current_price)
        if dividend_yield is None:
            dividend_yield = cached_dividend_yield
        if dividend_ttm is None:
            dividend_ttm = cached_dividend_ttm
        if not dividend_updated_at:
            dividend_updated_at = cached_dividend_updated_at

        upsert_symbol_snapshot(
            conn,
            symbol.symbol_code,
            industry,
            current_price,
            price_trade_date,
            dividend_yield,
            dividend_ttm,
            dividend_updated_at,
        )
        history = load_symbol_history(conn, symbol.symbol_code, start_date)
        if history.empty:
            primary_percentile = None
            dividend_ok = dividend_passes_threshold(dividend_yield, dividend_threshold)
            is_watch_candidate = False
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
                }
            )
            continue

        history = history.sort_values("trade_date").reset_index(drop=True)
        latest_trade_date = history.iloc[-1]["trade_date"]
        pe_series = pd.to_numeric(history["pe_ttm"], errors="coerce")
        pb_series = pd.to_numeric(history["pb"], errors="coerce")
        pe_clean = pe_series[pe_series.notna() & (pe_series > 0)]
        pb_clean = pb_series[pb_series.notna() & (pb_series > 0)]
        pe_percentile = percentile_of_current(pe_series)
        pb_percentile = percentile_of_current(pb_series)
        primary_percentile = primary_indicator_percentile(primary_indicator, pe_percentile, pb_percentile)
        dividend_ok = dividend_passes_threshold(dividend_yield, dividend_threshold)
        primary_ok = primary_percentile is not None and primary_percentile <= threshold
        if strategy_mode == "strict":
            is_watch_candidate = bool(primary_ok and dividend_ok is True)
        else:
            is_watch_candidate = bool(primary_ok)

        rows.append(
            {
                "display_name": symbol.display_name,
                "symbol_code": symbol.symbol_code,
                "industry": industry,
                "primary_indicator": primary_indicator,
                "latest_trade_date": latest_trade_date,
                "current_price": current_price,
                "pe_ttm": safe_float(pe_series.iloc[-1]),
                "pe_ttm_10y_percentile": pe_percentile,
                "pb": safe_float(pb_series.iloc[-1]),
                "pb_10y_percentile": pb_percentile,
                "dividend_yield": dividend_yield,
                "dividend_ttm": dividend_ttm,
                "dividend_updated_at": dividend_updated_at,
                "dividend_pass": dividend_ok,
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
            "sample_size": "有效样本数",
            "pe_below_20pct": "滚动市盈率是否低估",
            "pb_below_20pct": "市净率是否低估",
            "主指标近10年分位": "建议主看指标近10年分位",
            "is_watch_candidate": "是否进入重点观察区间",
            "min_percentile": "市盈率市净率较低分位",
            "低估判断依据": "低估判断依据",
            "综合低估结论": "综合低估结论",
        }
    )

    for col in ["所属行业", "建议主看指标", "低估判断依据", "综合低估结论", "股息数据更新日"]:
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
    for col in ["滚动市盈率近10年分位", "市净率近10年分位", "建议主看指标近10年分位", "市盈率市净率较低分位", "当前股息率"]:
        display_df[col] = display_df[col].map(format_percent)
    for col in ["当前价格", "当前滚动市盈率", "当前市净率", "近12个月每股分红"]:
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


def build_email_message(config: dict, html_body: str, alerts_path: Path, candidate_count: int) -> EmailMessage:
    email_cfg = config.get("email") or {}
    strategy_cfg = config.get("strategy") or {}
    required_keys = ["smtp_host", "smtp_port", "username", "password", "from_addr", "to_addrs"]
    missing = [key for key in required_keys if not email_cfg.get(key)]
    if missing:
        raise ConfigError(f"Missing email config keys: {', '.join(missing)}")

    report_date = date.today().strftime("%Y-%m-%d")
    dividend_threshold = float(strategy_cfg.get("dividend_yield_threshold", 0.045))
    message = EmailMessage()
    message["Subject"] = f"股票池每日估值结果 {report_date}"
    message["From"] = email_cfg["from_addr"]
    message["To"] = ", ".join(email_cfg["to_addrs"])
    message.set_content(
        "\n".join(
            [
                f"日期：{report_date}",
                f"本次进入重点观察区间标的数：{candidate_count}",
                "判断口径：PB为主、股息率补充、PE辅助。",
                f"股息率阈值：{dividend_threshold:.1%}",
                "请查看 HTML 正文或附件 alerts.csv。",
            ]
        )
    )
    message.add_alternative(html_body, subtype="html")
    message.add_attachment(
        alerts_path.read_bytes(),
        maintype="text",
        subtype="csv",
        filename=alerts_path.name,
    )
    return message


def send_email(config: dict, html_path: Path, alerts_path: Path, candidate_count: int) -> None:
    html_body = html_path.read_text(encoding="utf-8")
    message = build_email_message(config, html_body, alerts_path, candidate_count)
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

        email_enabled = bool((config.get("email") or {}).get("enabled", False))
        candidate_count = int(summary["is_watch_candidate"].sum()) if not summary.empty else 0
        should_send = email_enabled and (not args.skip_email)
        email_error = None
        if should_send:
            try:
                send_email(config, html_path, alerts_path, candidate_count)
            except Exception as exc:
                email_error = exc

        print(f"Saved CSV: {csv_path}")
        print(f"Saved HTML: {html_path}")
        print(f"Saved alerts: {alerts_path}")
        print(f"Strategy mode: {strategy_mode}")
        print(f"Dividend threshold: {dividend_threshold:.2%}")
        print(f"Candidates: {candidate_count}")
        if should_send and email_error is None:
            print(f"Email alerts sent for this run with {candidate_count} candidates")
        elif should_send:
            print(f"Email alerts failed: {email_error}")
        elif not email_enabled:
            print("Email alerts skipped because email is disabled")
        else:
            print("Email alerts skipped because --skip-email was provided")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
