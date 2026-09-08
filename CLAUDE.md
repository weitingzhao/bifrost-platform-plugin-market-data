# CLAUDE.md — bifrost-platform-plugin-market-data

与本项目用户对话一律使用中文回复；UI 字符串与代码标识符使用 English。

## 工作区定位（2026-09-06）

| 项 | 值 |
|---|---|
| 域 / 载荷 | Ops · Subcontractor（供数插件）· Polygon → `raw_market.*` / `ops_jobs.*`（Golden Source），与 Research 的数据流方向相反 |
| 运行位置 | K3s `plugin-market-data` NS，API `:8790`（Trade 经 `/api/plugin/market-data/`）；`bifrost-build-market-data` 流水线 |
| 秘密 | Polygon API key 在未跟踪的 `.env` / K8s Secret 里，永不入库 |
| 仓库可见性 | GitHub **PUBLIC**（12 个 repo 全部公开）—— `.env`、Secret YAML、dump、kubeconfig、账户内容永不入库 |
| 硬边界 | D10 交易执行冻结（BLOCKED）· D13 三域边界 · 平台/业务解耦（Flywheel A/B） |
| 事实基线 | `../AGENT_FACTS.md`（§8c 运行时与安全事实）· 规则 `../CLAUDE.md`（§8 Claude Code 运行配置） |

会话请在工作区根 `/stocks` 启动（加载治理层 hooks / auto mode / 共享记忆）；运行时与安全事实以 `../AGENT_FACTS.md` §8c 为准。

## 职责

**`bifrost-market-data`** — Bifrost Ops Platform 的 **Market Data Subcontractor**。
从 Trade System 剥离的 Polygon.io 公共市场数据采集与固化层。

| 组件 | 说明 |
|------|------|
| Polygon REST ingest | 股票/期权日线、快照、合约目录、基本面、公司行动 → `market.*` |
| PG-as-broker workers | `ops_jobs.job_ingest` + asyncio Deployment（无 Celery） |
| CronJob scheduler | 替代 Celery Beat，定时 enqueue |

## 架构边界

- **Platform core** (`bifrost-platform`): 通用环境治理 — matrix、spine、Console
- **本 repo**: 独立进程、独立 K8s namespace、通过 PostgreSQL schema 契约与消费者解耦
- **Trade** (`bifrost-trade-*`): 只读 `raw_market.*`；不直连 Polygon；不写 `ops_jobs.*`（`data_ops` schema **retired Wave 8**）
- **不含 IB**：无 TWS/Gateway/bars IB 路径

## 数据库（Golden Source 模式）

- **单一 Golden Source 数据库**：`bifrost_golden_source`（CNPG 管理）
- 所有 Trade 环境共享同一数据库实例，不再按环境分离
- Schema：`raw_market.*`（公共行情）+ `ops_jobs.*`（作业队列 / freshness / **data_source_void**）；`features.*` 由 **bifrost-research** 写入
- DDL 归 **本 repo** 管理（`src/bifrost_market_data/schema/ddl.py`）
- 不依赖 `bifrost-core`；不 import `bifrost-trade-*` Python 包
- **PG 目标**：CNPG LAN NodePort `192.168.10.73:30432`（见 `config/market-data.yaml.example`）
 - 覆盖：`POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_PASSWORD` 等环境变量
 - 本机 `localhost:5432` 不是默认目标
- Trade 消费者通过 Plugin API HTTP（`:8790` via `platform-api` `:8780`）读取，零直接 SQL
- **Readiness authority (0.7.9+)**: `/market/readiness/summary` · `/market/readiness/source-void` · ingest enqueue aliases (`snapshot_backfill` / `grouped_daily_backfill` / `vendor_gap_fix`). Trade `preference_data_gap_ack` retired.

## K8s（Single Golden Source）

- Namespace: `plugin-market-data`（唯一活跃实例；STG/PROD overlays 已归档至 `k8s/overlays/_archived/`）
- Workers: `polygon-worker-stocks` / `polygon-worker-options`
- API: `market-data-api` Service `:8790`（`GET /health`, analytics under `/market/analytics/*`）
- Watchlist union mode: 通过 `platform-api` 聚合所有 Trade 环境 watchlist
- Console 治理: Subcontractors → Plugin Gallery（catalog 在 P6 注册）

## 命令

```bash
make install-dev
make lint
make test
make db-init              # schema apply (+ best-effort roles)
make apply-roles          # create_roles.sql (needs elevated PG role)
make run-api              # Plugin API on :8790
make verify-market-data   # P6: K8s deploy + health + CronJobs + platform probe
make sync-platform-write-token  # copy write-token → platform-stg/prod for Console enqueue
make sync-write-auth-overlay    # ConfigMap overlay of deps.py (X-Market-Data-Write-Token on image 0.3.2)
```

## 订阅事实（2026-09-06 实测）

Owner 订阅 **Options Starter + Stocks Starter + Financials & Ratios**：无限调用（限流是自伤，`tier: starter` 已改为 8 req/s 软上限）；股票聚合滚动 5 年、期权聚合滚动 2 年；trades / quotes / last-trade / 指数行情 **403**（升级前不拉）。
程序文档：`docs/SUBSCRIPTION_FOCUS_PROGRAM.md`。

`option_snapshot.snapshot_ts` 是**观测时间**（EOD = 该 session 的 16:00 NY 锚点），不是最后成交时间——后者在 `last_trade_ts`。链快照只反映当前会话，所以补跑只在下一次开盘前有效（`trading_calendar.chain_session`）。

**先跑 doctor 再猜**：`GET /market/doctor` 给出「本 session 应有 vs 实有」与每项处方；`POST /market/doctor/heal`（写 token）执行处方。Console Ingest tab 的 Doctor 面板与 MCP `market_data_doctor` / `market_data_heal` 是同一份处方；Dagster `market_self_heal` 每晚 00:45 UTC 自动跑一遍。

## 修改纪律

- 公开表/字段契约变更需同步 Trade 消费者 + Ops Console catalog
- 不引入 Celery / Redis broker 作为任务路由（Redis 仅心跳/缓存可选）
- D10 BLOCKED — 不涉及交易执行路径
