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

## 数据库

- 共用物理 PostgreSQL 实例（`bifrost_dev` / `bifrost_prod`）
- Schema：`market.*`（公共行情）+ `data_ops.*`（作业队列 / freshness）
- DDL 归 **本 repo** 管理（`src/bifrost_market_data/schema/ddl.py`）
- 不依赖 `bifrost-core`；不 import `bifrost-trade-*` Python 包

## K8s

- Namespace: `plugin-market-data`
- Workers: `polygon-worker-stocks` / `polygon-worker-options`
- Console 治理: Subcontractors → Plugin Gallery（catalog 在 P6 注册）

## 命令

```bash
make install-dev
make lint
make test
make db-init              # schema apply
make verify-market-data   # P6: K8s deploy + health + CronJobs + platform probe
```

## 修改纪律

- 公开表/字段契约变更需同步 Trade 消费者 + Ops Console catalog
- 不引入 Celery / Redis broker 作为任务路由（Redis 仅心跳/缓存可选）
- D10 BLOCKED — 不涉及交易执行路径
