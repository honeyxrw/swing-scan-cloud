# 波段选股云端扫描

不依赖本地电脑开机，由 GitHub Actions 在云端定时运行全市场扫描，结果自动更新网页。

## 结构

- `scripts/full_market_screener.py` — 扫描脚本（纯 Python 标准库，数据源为腾讯行情接口）
- `docs/index.html` — 交易辅助台（含 🔥今日推荐）
- `docs/pool.json` — 扫描结果（Actions 自动提交）
- `.github/workflows/scan.yml` — 定时任务

## 网页地址

GitHub Pages：`https://<用户名>.github.io/<仓库名>/`
推送 main 后自动发布，无需额外部署步骤。

## 运行节奏（北京时间）

| 时间 | 模式 | 内容 |
|---|---|---|
| 交易日 11:40 | midday | 午间快照：今日推荐 10 只 + 精选 1 只 |
| 交易日 15:15 | close | 收盘完整版：今日推荐 20 只 |

## 维护注意

本地 `monitoring/scripts/full_market_screener.py` 与本仓库 `scripts/` 是同一份脚本的两份拷贝：
**改完本地脚本后，必须复制到本仓库并推送**，否则云端仍跑旧逻辑。
