---
version: 2026-09-09.2
updated: 2026-09-09
status: 三维已可读 · 广度基本拉满 · 深度是唯一的大洞
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
| C-B1 | ⚠️ | 契约表已落地（`contracts.py`，19 个数据集各自声明档位与分母），`/market/coverage/dimensions` 只读它。仍是 ⚠️：**旧的 4 套分母还在原地**——`api/coverage.py:253` watchlist（默认 80）、`doctor.py:287-289` optionable watchlist ∪ 基准、`quality.py:156` 硬编码常数 4000、`api/readiness_summary.py:44-52` `v_us_equity_universe`。收敛它们是下一步。 |
| C-B2 | ✅ | `GET /market/coverage/dimensions` 同时报意图达成率与口径利用率，两者分列不合并。`/market/capabilities`（`subscription.py:105`）回答的是"哪些能力被实现了"，不是"持有了多少"；Console 的 Capability 面板 `capPct` 是手工标注的 implemented/partial 计数。 |
| C-B3 | ⚠️ | Doctor 已区分"计划不覆盖"（`doctor.py:463-469`，error 含 `not entitled` 时不给 retry 处方）与可重试失败。但"vendor 无此数据"与"未排程"两类未区分：`ops_jobs.symbol_source_void`（`ddl.py:644`）记了前者却不在任何覆盖率分母里体现。 |
| C-D1 | ✅ | 每个数据集在 `contracts.py` 的 `DepthTarget` 里声明窗口，取自订阅口径（stocks 5 年 / options 2 年 / financials 2009）或 tier 要求（24/12 个月），并带上"为什么是这个数"。旧的散落常量（`coverage.py:660` `years=5`、`schedule.yaml:117` `months: 24`）仍在各自的调用点，但不再是深度的事实源。 |
| C-D2 | ✅ | `/market/coverage/dimensions` 按标的度量并汇总：达标标的数、深度中位数、最浅的是谁（§2b）。查询形状按基数选——跳跃扫描只在不同值少时才划算（option_daily 60 个 0.85 秒，stock_daily 20,695 个则要 152 秒，而一次分组扫描 24 秒）。旧的 `StockDepthSection.tsx` 仍只覆盖 80 个 watchlist 标的且主视觉是缺口数，应由三维表取代。 |
| C-D3 | ⚠️ | `doctor.py:389-395` 已正确表达"vendor snapshot 是 point-in-time，补跑会落到今天"；`SNAPSHOT_COVERAGE_MIN=0.90`（`doctor.py:54`）也正确记录了"95% 结构上不可达"。但 **ratios 端点忽略 `date` 参数、历史只能向前累积**这条边界没有写进任何地方。 |
| C-D4 | ⚠️ | `option_daily` 一行现在就是进度条：45/575 广度、58/70 达标、中位 24 个月（§2b）。仍是 ⚠️：它答得出"买到了多少"，答不出"按当前速率还要多久"——那需要把队列消化速率接进来。 |
| C-F1 | ❌ | **"当期 session" 有 4 个定义**：`doctor.py:244-261` `resolve_session`（19:30 NY）、`quality.py` `fetch_completed_trading_days`（排除今天）、`api/ingest_dashboard.py` 的 22:30 ET grace、`api/readiness_summary.py:39-40` `_STALE_DAYS=7`。 |
| C-F2 | ⚠️ | Doctor 的 `STALENESS`（`doctor.py:69-75`）已按 slot 分别声明（calendar 48h、option-refresh 12h、corporate 168h、fundamentals-rotate 48h），是四套里最接近契约的一套；但它只覆盖 5 个 slot，其余数据集共用一个 24/72 小时。 |
| C-F3 | ❌ | **4 套阈值并存**：`quality.py:14-16`（24h / 周末 72h）、`doctor.py:69-75`（12/48/168h）、platform-api 的 `freshnessWeekendMaxAgeH`、Console `dataVitalsModel.ts:9-10`（12h / 72h）。同一个数据集在不同面板上可以显示不同健康状态。 |
| C-F4 | ⚠️ | 0.18.1 起 `SESSION_ONCE_SLOTS` 的守卫改为按覆盖判定并排除盘中行（`scheduler/daily.py`），跳过不再等于成功。但 `_slot_adherence`（`api/ingest_dashboard.py:513-711`）仍以 job 证据判定 `on_plan/missed`，未对数据判定。 |
| C-G1 | ❌ | 存在结构上永远等于 100% 的分母：`DataInventoryStrip.tsx:92` 用 `max(watchlist, 实际值)`、`OptionCoverageSection.tsx:191` 用"最大的那个 underlying"。这些不是覆盖率。 |
| C-G2 | ⚠️ | 19 个数据集全部报出三个轴（`/market/coverage/dimensions`）。仍是 ⚠️：`stock_movers` 是 top-N 榜单，用全市场当分母得到 0.4%，是无意义的比率——它需要自己的档位。 |
| C-G3 | ❌ | entitled 但未持有的 8 个数据面（§4）在任何界面上都不可见；`/market/capabilities` 只列 planned（需升级）与 unavailable（端点 404），不列"已付费但没在采"。 |

## 2b. 三维首次读数（2026-09-09，`/market/coverage/dimensions`）

契约表落地后第一次全量读数。**每个百分比的分子都取自它自己的分母集合**（"该档位要的标的里，我持有多少"），范围外的持有量另列，不灌进比率。

| 数据集 | 档位 | 广度 | 范围外 | 深度 |
|---|---|---|---|---|
| `stock_daily` | whole-market | **97.5%**（5,182/5,317） | +7,336 | 10,862/20,695 达标，中位 60 个月 |
| `stock_snapshot` | whole-market | 99.8% | +7,839 | current only |
| `ticker` | whole-market | 100.0% | +61 | catalogue |
| `short_interest` | whole-market | 99.3% | +17,654 | forward only |
| `short_volume` | whole-market | 98.1% | +9,942 | forward only |
| `ratios` | whole-market | **74.7%** | +828 | forward only |
| 财报三表 | whole-market | 82–83% | +48 | 0/4,468 达标，中位 118 个月 |
| `corporate_action` | whole-market | 13.8% | +3,207 | catalogue |
| `stock_movers` | whole-market | 0.4% | +19 | current only |
| `option_snapshot` / `option_open_interest` | universe | **99.1%**（570/575） | 0 | 0/570 达标，中位 0 个月 |
| `option_contract` | universe | 99.1% | 0 | catalogue |
| `option_daily` | universe | **7.8%**（45/575） | +1 | **58/70 达标，中位 24 个月** |
| `stock_minute` | benchmark-only | **27.3%**（3/11） | +15 | 0/18 达标 |
| `option_minute` | benchmark-only | **9.1%**（1/11） | +7 | 0/18 达标 |

读出来的四件事：

1. **whole-market 档基本已拉满订阅口径**（97–100%），"没有充分利用订阅"这句话对股票面不成立。范围外的七千多个是五年里的退市历史，被正确排除而不是灌成 389%。
2. **`ratios` 的 74.7% 不是我们的缺口** —— vendor 的 ratio 覆盖本就只到约 5,000 个 ticker，这是供应商边界。
3. **`option_daily` 7.8% 是那 324 万个回填任务的进度条**，第一次可见：已到达的 70 个标的里 58 个达到 24 个月目标，中位 24 个月——回填本身是对的，只是才走到 70/575。
4. **新发现：分钟线只覆盖 11 个基准里的 3 个**（`stock_minute` 27.3%、`option_minute` 9.1%），而它持有的 18 个标的中 15 个在基准之外。`minute-bars` 轮转的是 watchlist，不是它声称的基准集。

建模过程中被这次读数抓出并修正的四个错误（都在契约表侧，记录以免重犯）：分子分母窗口不一致（389%）、benchmark 分母只算基准漏了 watchlist（164%）、`corporate_action` 的窗口向前看导致"深度"为负、`option_open_interest` 在分区表上跳跃扫描超时。

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
| 2026-09-09.2 | 2026-09-09 | 契约表代码化（`contracts.py`，19 个数据集）+ `/market/coverage/dimensions` + Console 三维表。三维首次可读（§2b）。契约状态 ✅ 3 / ⚠️ 5 / ❌ 6。 |
| 2026-09-09.1 | 2026-09-09 | 基线。三维首次实测；14 条契约中 ✅ 0 / ⚠️ 4 / ❌ 10。深度维度确认为最大空白（575 个标的里 47 个有期权历史，零面板显示）。 |
