# 日常操作清单

## 快速开始

### 本地更新后

先在 Windows PowerShell 执行：

```powershell
D:\ProgramData\Backup\stock-monitor\update_server.bat
```

### 服务器测试

登录服务器后执行：

```bash
/opt/stock-monitor/test_run.sh
```

### 立刻真实执行并发邮件

```bash
/opt/stock-monitor/run_daily_valuation.sh
```

### 查看最近日志

```bash
tail -n 50 /opt/stock-monitor/cron.log
```

### 查看定时任务

```bash
crontab -l
```

当前正式定时任务应为：

```bash
0 17 15 * * /opt/stock-monitor/run_daily_valuation.sh
```

---

## 1. 本地改完后上传到服务器

在 Windows PowerShell 中执行：

```powershell
D:\ProgramData\Backup\stock-monitor\update_server.bat
```

上传成功会看到：

```text
Upload completed.
```

如果失败会看到：

```text
Upload failed.
```

---

## 2. 上传后先做不发邮件测试

登录 Linux 服务器后执行：

```bash
/opt/stock-monitor/test_run.sh
```

正常情况下会看到类似：

```text
Saved CSV: output/latest_valuation.csv
Saved HTML: output/latest_valuation.html
Saved alerts: output/alerts.csv
Candidates: 0
Email alerts skipped because --skip-email was provided
```

这表示：

- 股票池读取正常
- 脚本能正常运行
- 报表能正常生成
- 这次只是测试，没有发邮件

---

## 3. 需要立即真实执行并发邮件时

在服务器执行：

```bash
/opt/stock-monitor/run_daily_valuation.sh
```

然后查看日志：

```bash
tail -n 50 /opt/stock-monitor/cron.log
```

如果成功，日志里会看到类似：

```text
Email alerts sent for this run with 0 candidates
```

这表示即使当前候选数为 0，也已经按规则发出邮件。

---

## 4. 平时查看最近运行日志

```bash
tail -n 50 /opt/stock-monitor/cron.log
```

---

## 5. 查看定时任务

```bash
crontab -l
```

当前正式定时任务应为：

```bash
0 17 15 * * /opt/stock-monitor/run_daily_valuation.sh
```

含义：每月 15 号 17:00 自动执行一次。

---

## 6. 服务器上的关键文件

服务器目录：

```bash
/opt/stock-monitor
```

关键文件：

- `monitor_stock_valuation.py`：主脚本
- `monitor_config.yaml`：配置文件
- `stocks.txt`：股票池
- `run_daily_valuation.sh`：正式每日执行脚本
- `test_run.sh`：测试脚本（不发邮件）
- `.env`：邮箱密码环境变量文件
- `cron.log`：运行日志
- `data/`：缓存数据
- `output/`：输出报表

---

## 7. 如果以后只改股票池

本地修改：

```text
D:\ProgramData\Backup\stock-monitor\stocks.txt
```

然后重新上传：

```powershell
D:\ProgramData\Backup\stock-monitor\update_server.bat
```

再到服务器测试：

```bash
/opt/stock-monitor/test_run.sh
```

---

## 8. 如果以后改了依赖

如果你改了 `requirements.txt`，上传后还需要在服务器执行：

```bash
cd /opt/stock-monitor
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 9. 如果出现问题，优先执行这两个命令

### 先跑测试

```bash
/opt/stock-monitor/test_run.sh
```

### 再看日志

```bash
tail -n 50 /opt/stock-monitor/cron.log
```

把这两段输出发给 Claude，通常就能继续排查。

---

## 10. 当前运行规则

- 每月 15 号 17:00 自动执行
- 使用 `stocks.txt` 作为股票池
- 每次运行都发邮件
- 即使候选数量没有变化，也照样发邮件
- 即使候选数量为 0，也照样发邮件
