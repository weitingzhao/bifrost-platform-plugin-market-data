---
version: 2026-09-09.1
updated: 2026-09-09
status: 基线快照 · 深度维度尚未被任何面板回答
---

# Massive 校准

> **这是现状，不是目标。** 目标在《Massive 蓝图》。这份文档回答"今天离蓝图多远、差在哪"，每次校准都会变。
> 契约编号在蓝图里定义，这里只记状态与证据。
> 同一份文件：仓库 `.../docs/MASSIVE_CALIBRATION.md`，API `GET /market/docs/calibration`。

状态：✅ 满足　⚠️ 部分　❌ 不满足　⏳ 代码已到位等数据

## 0. 为什么需要这份校准

2026-09-08 之前，`eod-pipeline` 每个交易日都返回 `ok: true, skipped: evidence_exists`。job 层面全绿、排程依从性全绿、Doctor 不报警，而期权链的实际覆盖是 **26 / 575**，已持续数周。根因是 `SESSION_ONCE_SLOTS` 的守卫问"今天有没有任何非失败的 `option_snapshot` 任务"，而盘中链从 15:30 ET 起写的正是同一个 kind、同一个 `trade_date`——**跳过被记成了成功**（已在 0.18.1 修复）。

这类故障不是监控没做好，是**度量的单位错了**。本校准把单位从"任务"换成"持有的数据"。

## 1. 今日三维读数（2026-09-09 00:30 UTC 基线）

### 1.1 广度

| 口径 | 数值 |
|---|---|
| vendor 活跃 ticker（whole-market 档分母） | **5,317** |
| `research.option_universe`（universe 档分母） | **575**（resident 27 / core 527 / edge 21） |
| 实际枚举出合约的标的 | **570** |
| `iv_radar_benchmarks`（benchmark-only 档分母） | 11 |

- **universe 档的意图达成率 570 / 575 = 99.1%**；缺的 5 个是 vendor 无期权链（C-B3 的第一类归因，不是故障）。
- **universe 档的口径利用率 575 / 5,317 = 10.8%** —— 这是"没有充分利用订阅"的那个数，但它是**规则决定的**，不是采集失败。
- whole-market 档：`stock_daily` 20,691 个标的（>5,317，因为含已退市历史）、`stock_snapshot` 13,317、`stock_financials` 23,381、`corporate_action` 3,943。

### 1.2 深度

| 数据集 | 实测 | 目标 |
|---|---|---|
| `option_daily` | **仅 47 个标的有任何历史**（resident 25 / core 19 / edge 0）；有数据者平均 21.9–22.2 个月，最浅 0.7 个月 | resident/core 24 个月、edge 12 个月 |
| `option_snapshot` | 570 个标的，2026-08-05 → 09-08（34 天） | 保留 90 个 session（trim 策略，非缺陷） |
| `option_open_interest` | 570 个标的，2026-07-06 → 09-08 | 跟随快照 |
| `stock_financials` | 23,381 个标的，2009-04-15 → 2026-09-04 | 2009 年起 ✅ |
| `corporate_action` | 3,943 个标的，1980-10-09 → 2026-11-06 | −7/+60 天窗口（历史来自早期一次性导入） |
| `stock_minute` | 269,405 行 | 滚动 12 个月 |
| `treasury_yield` | 1,251 行 | 滚动 30 天 |
| `option_trades` | 0 行 | 订阅不覆盖，非缺陷 |

**528 / 575 个标的没有任何期权日线历史。** 这正是当前 324 万个回填任务（约 34 小时队列）在买的东西，而没有任何面板显示这件事。

### 1.3 新鲜度

- 2026-09-08 晚间**首次**全宇宙 EOD 快照：写入 **566 个标的**（此前每日 26）。
- `stock_daily` 每日全市场 grouped，当日到位。
- 队列消化 1,500–1,740 任务/分，在飞 35，失败率 0–1/分。

## 2. 逐契约状态

| 编号 | 状态 | 证据 |
|---|---|---|
| C-B1 | ❌ | **同时存在 4 套"应该有多少标的"的分母**：`api/coverage.py:253` watchlist（默认 80）、`doctor.py:287-289` optionable watchlist ∪ 基准、`quality.py:156` 硬编码常数 4000、`api/readiness_summary.py:44-52` `v_us_equity_universe`。没有契约表。 |
| C-B2 | ❌ | 两个比率一个都没有。`/market/capabilities`（`subscription.py:105`）回答的是"哪些能力被实现了"，不是"持有了多少"；Console 的 Capability 面板 `capPct` 是手工标注的 implemented/partial 计数。 |
| C-B3 | ⚠️ | Doctor 已区分"计划不覆盖"（`doctor.py:463-469`，error 含 `not entitled` 时不给 retry 处方）与可重试失败。但"vendor 无此数据"与"未排程"两类未区分：`ops_jobs.symbol_source_void`（`ddl.py:644`）记了前者却不在任何覆盖率分母里体现。 |
| C-D1 | ❌ | 深度目标散落在代码常量里：`coverage.py:660` `years=5`、`schedule.yaml:117` `months: 24`、`subscription.py:23-43` 的窗口字符串只是展示文案。无契约表。 |
| C-D2 | ❌ | 唯一的深度面板 `StockDepthSection.tsx` 只覆盖 watchlist 的 80 个标的（`MarketDataCoverageTab.tsx:126-131` 不带 limit → 服务端默认 80），靠 N×2 并发拉取，且主视觉是缺口数（`:322-323, 342`）而非深度——"往回多少年"只出现在 tooltip 与折叠表格里。**期权深度零面板。** |
| C-D3 | ⚠️ | `doctor.py:389-395` 已正确表达"vendor snapshot 是 point-in-time，补跑会落到今天"；`SNAPSHOT_COVERAGE_MIN=0.90`（`doctor.py:54`）也正确记录了"95% 结构上不可达"。但 **ratios 端点忽略 `date` 参数、历史只能向前累积**这条边界没有写进任何地方。 |
| C-D4 | ❌ | 回填进度不可见。`IngestDailyVolume` 显示的是 job 条数（09-08 那 356 万），无法回答"买到了多少个标的的多少个月"。 |
| C-F1 | ❌ | **"当期 session" 有 4 个定义**：`doctor.py:244-261` `resolve_session`（19:30 NY）、`quality.py` `fetch_completed_trading_days`（排除今天）、`api/ingest_dashboard.py` 的 22:30 ET grace、`api/readiness_summary.py:39-40` `_STALE_DAYS=7`。 |
| C-F2 | ⚠️ | Doctor 的 `STALENESS`（`doctor.py:69-75`）已按 slot 分别声明（calendar 48h、option-refresh 12h、corporate 168h、fundamentals-rotate 48h），是四套里最接近契约的一套；但它只覆盖 5 个 slot，其余数据集共用一个 24/72 小时。 |
| C-F3 | ❌ | **4 套阈值并存**：`quality.py:14-16`（24h / 周末 72h）、`doctor.py:69-75`（12/48/168h）、platform-api 的 `freshnessWeekendMaxAgeH`、Console `dataVitalsModel.ts:9-10`（12h / 72h）。同一个数据集在不同面板上可以显示不同健康状态。 |
| C-F4 | ⚠️ | 0.18.1 起 `SESSION_ONCE_SLOTS` 的守卫改为按覆盖判定并排除盘中行（`scheduler/daily.py`），跳过不再等于成功。但 `_slot_adherence`（`api/ingest_dashboard.py:513-711`）仍以 job 证据判定 `on_plan/missed`，未对数据判定。 |
| C-G1 | ❌ | 存在结构上永远等于 100% 的分母：`DataInventoryStrip.tsx:92` 用 `max(watchlist, 实际值)`、`OptionCoverageSection.tsx:191` 用"最大的那个 underlying"。这些不是覆盖率。 |
| C-G2 | ❌ | 14 个数据集中，能回答全部三个轴的：**0 个**。能回答两个轴（广度+新鲜度）的：`stock_daily`、`option_snapshot`。 |
| C-G3 | ❌ | entitled 但未持有的 8 个数据面（§4）在任何界面上都不可见；`/market/capabilities` 只列 planned（需升级）与 unavailable（端点 404），不列"已付费但没在采"。 |

## 3. 已知差距与最小改动

### 3.1 重复与冲突（最大的一类）

| 差距 | 证据 | 最小改动 |
|---|---|---|
| 新鲜度被至少 **7 个面板**回答 | `WorkersFreshnessPanel` / `DataVitalsStrip` / `SepaStatsSection` / `QualityScoreSection` / `QueueDashboardPanel` / `DoctorPanel` / `HusbandryStrip` | 收敛到契约表的截止时间，面板只渲染 |
| 陈旧阈值 4 套 | 见 C-F3 | 单一来源，其余读它 |
| session 定义 4 套 | 见 C-F1 | 统一到 `doctor.resolve_session` |
| 分母 4 套 + 2 个永远 100% 的假分母 | 见 C-B1 / C-G1 | 契约表 |
| `/coverage/quality-score` 被三处 fetch | `MarketDataOverviewTab.tsx:68-73`、`QualityScoreSection.tsx:46-51`（共享缓存）、`ReadinessPanel.tsx:70-75`（**独立 queryKey，真重复请求**） | 统一 queryKey |
| `/coverage/contracts` 被两处不同 limit 打两次 | `DataVitalsStrip.tsx:105-110`（默认 100）与 `OptionCoverageSection.tsx:159`（500）——Vitals 卡上的 contracts 只是前 100 名之和，与 Coverage 页对不上 | 同一 limit 或同一 query |
| 纯 alias 端点 | `coverage.py:701-707` `stock-day-quality-detail` = `bar-quality-detail` | 删一个 |

### 3.2 深度的空白

| 差距 | 原料在不在 | 最小改动 |
|---|---|---|
| 期权深度（每 underlying 往回多少月） | **在**：`coverage.py:913-954` `query_distributions`，Console 零消费 | 一条按 tier 聚合的查询 |
| 全市场 per-symbol 股票深度 | **在**：`readiness_data.py:86-128` 不带 `summary=true` 时返回每标的 `first_bar_date` / `last_bar_date` | Console 只用了 `summary=true`，改为消费明细 |
| `option_snapshot` 的 session 深度 | 不在（`snapshot-quality-detail` 只到 30 天且单标的） | 新查询 |
| 基本面 / ratios / short_* 的时间深度 | 不在 | 新查询 |
| **订阅窗口利用率**（5y / 2y / 2009 当分母） | 不在——`subscription.py:23-43` 有窗口字符串但只是文案 | 契约表 + 一条查询 |
| `ingest_freshness` 无历史序列 | 表 PK 是 `dimension`，每次 UPSERT 覆盖（`freshness.py:79-89`） | 画不出趋势；`IngestDailyVolume` 用 job_ingest 代偿但只有约 7 天（trim） |

### 3.3 采集侧缺口

| 编号 | 差距 | 证据 |
|---|---|---|
| G1 | `option-backfill` 有 slot、有 handler、有订阅，但**没有任何排程**，只能手工触发 | 不在 `market_slot_schedules.py` 的 `_MARKET_SPECS`；`schedule.yaml:115-125` 刻意不写 cron |
| G2 | `ticker_type` 有 handler、有表，但没有任何 slot 入队 | `ingest/__init__.py:65` 有 handler；`daily.py` 无 `_add("ticker_type", …)` |
| G4 | 按 symbol 的除权除息无补采路径 | `corporate` slot 只发 `*_market`（`daily.py:1230-1231`），窗口 −7/+60 天 |
| G8 | `fundamentals-rotate` 关掉了三个 entitled 的按 symbol 拉取 | `schedule.yaml:84-86` `include_ratios` / `include_short_interest` / `include_short_volume` 全 `false` |
| G11 | `polygon-ws` Deployment 在跑，但拉的是当前 403 的数据面且不落库 | `deployment-polygon-ws.yaml:14` `replicas: 1`；`ws/redis_writer.py` 只写 Redis |

## 4. 待决清单（entitled 但未开采）

蓝图 §6 列了 8 项。**要不要采由 Owner 逐项拍板**，本节只记录它们的状态：全部为「端点可用、无 handler、无表、无 slot」，`ticker_type` 例外（有 handler 有表，缺 slot）。

排序建议（按能解锁的下游能力）：

1. **新闻** —— Research 蓝图 §3.2 的事件面目前显示"未测"，且**没有任何前瞻事件源**；这是六个面里唯一完全黑的一面。
2. **ticker events / IPO** —— 回测的幸存者偏差修正。
3. **技术指标** —— 与 vendor 口径一致，但 Research 已自算，边际价值最低。

## 5. 校准记录

| 快照 | 日期 | 说明 |
|---|---|---|
| 2026-09-09.1 | 2026-09-09 | 基线。三维首次实测；14 条契约中 ✅ 0 / ⚠️ 4 / ❌ 10。深度维度确认为最大空白（575 个标的里 47 个有期权历史，零面板显示）。 |
