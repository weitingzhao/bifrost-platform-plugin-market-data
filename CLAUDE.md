# CLAUDE.md — bifrost-platform-plugin-market-data

与本项目用户对话一律使用中文回复；UI 字符串与代码标识符使用 English。

## 职责

**`bifrost-market-data`** — Bifrost Ops Platform 的 **Market Data Subcontractor**。
从 Trade System 剥离的 Polygon.io 公共市场数据采集与固化层。

| 组件 | 说明 |
|------|------|
| Polygon REST ingest | 股票/期权日线、快照、合约目录、基本面、公司行动 → `market.*` |
| PG-as-broker workers | `data_ops.job_ingest` + asyncio Deployment（无 Celery） |
| CronJob scheduler | 替代 Celery Beat，定时 enqueue |

## 架构边界

- **Platform core** (`bifrost-platform`): 通用环境治理 — matrix、spine、Console
- **本 repo**: 独立进程、独立 K8s namespace、通过 PostgreSQL schema 契约与消费者解耦
- **Trade** (`bifrost-trade-*`): 只读 `market.*`；不直连 Polygon；不写 `data_ops.*`
- **不含 IB**：无 TWS/Gateway/bars IB 路径

## 数据库（Golden Source 模式）

- **单一 Golden Source 数据库**：`bifrost_golden_source`（CNPG 管理）
- 所有 Trade 环境共享同一数据库实例，不再按环境分离
- Schema：`market.*`（公共行情）+ `market_analytics.*`（衍生指标）+ `data_ops.*`（作业队列 / freshness）
- DDL 归 **本 repo** 管理（`src/bifrost_market_data/schema/ddl.py`）
- 不依赖 `bifrost-core`；不 import `bifrost-trade-*` Python 包
- **PG 目标**：CNPG LAN NodePort `192.168.10.73:30432`（见 `config/market-data.yaml.example`）
  - 覆盖：`POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_PASSWORD` 等环境变量
  - 本机 `localhost:5432` 不是默认目标
- Trade 消费者通过 Plugin API HTTP（`:8790` via `platform-api` `:8780`）读取，零直接 SQL

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
```

## 修改纪律

- 公开表/字段契约变更需同步 Trade 消费者 + Ops Console catalog
- 不引入 Celery / Redis broker 作为任务路由（Redis 仅心跳/缓存可选）
- D10 BLOCKED — 不涉及交易执行路径
