---
version: 2026-09-10.14
updated: 2026-09-10
status: 四轴普查落地 · Doctor 接上厚度轴（能发现的现在也能修） · Coverage 分层 + 档位×粒度矩阵 · 五类度量偏差已修 · 判定移入插件并向前记录，矩阵能说出「变差了」 · SEPA 面板退役（十张表从未读到过）
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
| C-B1 | ✅ | 契约表（`contracts.py`，19 个数据集各自声明档位与分母）是唯一事实源，`scopes.py` 是唯一读它的地方。四套旧分母已收敛：**doctor** 改读 `load_research_universe()`——**检查用的名单就是采集用的那一份**，28 → 575；**quality** 的常数 4000 与 doctor 的 12000 是同一个量的两个门槛，合并为 `contracts.STOCK_DAILY_MIN_SESSION_SYMBOLS = 12000`（19 个 session 实测 12,396–12,576，2026-08-11 的失败 session 只有 18 行）；**readiness** 的 `tickers_active_count` 改读 `scopes.active_tickers`（`universe_count` 保留 `v_us_equity_universe`，因为 SEPA 排名的是美股普通股，是另一个问题——今天两者都是 5,317）；`api/coverage.py:253` 的 `limit=80` **不是分母**，是一份抽样清单（见 C-D2）。收敛后第一次实测又抓到 benchmark 档自己的分母是错的：`benchmark_scope` 给 watchlist 加载器传了空的 scheduler 配置，加载器于是走 DB 路径去查 Golden Source 没有的 `public.watchlist`，警告进了日志、分母悄悄缩回 11 个基准（0.19.4 修，26）。 |
| C-B2 | ✅ | `GET /market/coverage/dimensions` 同时报意图达成率与口径利用率，两者分列不合并。`/market/capabilities`（`subscription.py:105`）回答的是"哪些能力被实现了"，不是"持有了多少"；Console 的 Capability 面板 `capPct` 是手工标注的 implemented/partial 计数。 |
| C-B3 | ⚠️ | Doctor 已区分"计划不覆盖"（error 含 `not entitled` 时不给 retry 处方）与可重试失败，**并新增了第三类**：期权链的存在性检查只对"vendor 列出了未到期合约"的标的判定——2026-09-08 的 session 里 CIX / EA / ISTR / NVR / SENEA 五个名字没有任何快照行，但它们本来就没有活合约，报成缺口是误判。仍是 ⚠️：`ops_jobs.symbol_source_void`（`ddl.py:644`）记的"vendor 无此数据"还不在任何覆盖率分母里体现。 |
| C-D1 | ✅ | 每个数据集在 `contracts.py` 的 `DepthTarget` 里声明窗口，取自订阅口径（stocks 5 年 / options 2 年 / financials 2009）或 tier 要求（24/12 个月），并带上"为什么是这个数"。旧的散落常量（`coverage.py:660` `years=5`、`schedule.yaml:117` `months: 24`）仍在各自的调用点，但不再是深度的事实源。 |
| C-D2 | ✅ | `/market/coverage/dimensions` 按标的度量并汇总：达标标的数、深度中位数、最浅的是谁（§2b）。查询形状按基数选——跳跃扫描只在不同值少时才划算（option_daily 60 个 0.85 秒，stock_daily 20,695 个则要 152 秒，而一次分组扫描 24 秒）。`StockDepthSection.tsx` 仍只覆盖 80 个 watchlist 标的且主视觉是缺口数，应由三维表取代——**更正**：它的 `Math.max(rows.length, 1)` 是 `ScoreRing` 的分区总数（ready + thin + blocked 三块加起来就是取回的行数），不是覆盖率分母，不该被当成假分母；真正的问题是页面没说这 80 个是 20,695 个里的抽样。 |
| C-D3 | ⚠️ | `doctor.py` 已正确表达"vendor snapshot 是 point-in-time，补跑会落到今天"；`SNAPSHOT_COVERAGE_MIN=0.90` 也正确记录了"95% 结构上不可达"。**ratios 端点忽略 `date`、历史只能向前累积**这条边界现在写在契约里（`DepthTarget("forward_only", why=...)`）。仍是 ⚠️，而且原因变了：**这条边界曾被错误地推广**——`short_volume` 也标了 `forward_only`，是从 ratios 抄的，实测它认 `?date` 且有两年窗口，已改为 `rolling_days` 并补齐。计划边界必须逐个实测，不能按邻居推定。 |
| C-D4 | ✅ | `option_daily` 一行是进度条（广度、达标数、深度中位数，§2b），而"按当前速率还要多久"由 `ops_jobs.queue_sample` 回答：每 5 分钟一行，记深度、入队量、消化量、最老待跑年龄与 handler 的 p50/p95，Console 的 Queue history 面板画成曲线（6h/24h/3d/7d）。这也补上了此前唯一的历史来源——`job_ingest` 的 trim 把已完成行压到 40,000 条，按现在的速率只有约 15 分钟。 |
| C-F1 | ✅ | `session.py` 是唯一定义（19:30 NY 锚点 + 交易日历），`doctor` 与 `quality` 都调用它，交易日探针可注入以保留既有测试接缝。`ingest_dashboard` 的 22:30 ET 是 **cron 宽限窗口**，回答"该点火了吗"，与"该持有哪个 session"是两个问题，刻意保留。 |
| C-F2 | ✅ | 19 个数据集各自在契约里声明截止时间；`quality.check_freshness` 按维度取 `deadline_for_dimension()`，返回体里每个维度带自己的 `deadline_hours`，平铺的 `max_age_hours` 已为 `None`。线上实测：stock_daily / option_snapshot / option_open_interest 各 2h，calendar 48h。**0.19.5 修掉一处遗漏**：`fundamentals_market` 用 `session_is_today` 当截止时间的替身，而它在纽约午夜翻面——夏令时是 04:00 UTC，比 04:30 UTC 发布槽早半小时。2026-09-09 04:10 UTC 实测到这条假 critical，现按契约声明的 30h 判定。 |
| C-F3 | ⚠️ | Plugin 侧已收敛：doctor 的 `STALENESS` 由 `contracts.staleness_by_slot()` 派生（一条测试断言两者逐条一致），`quality` 的 24h/72h 周末规则退役——问交易日历后周末例外根本不需要存在。仍是 ⚠️：**platform-api 的 `freshnessWeekendMaxAgeH` 与 Console `dataVitalsModel.ts:9-10` 还各有一份**。 |
| C-F4 | ⚠️ | 0.18.1 起 `SESSION_ONCE_SLOTS` 的守卫改为按覆盖判定并排除盘中行；0.19.3 起 Doctor 也真的巡检 575 个名字而不是 28 个——**这是"跳过不是成功"第一次有牙齿**：26/575 这种 session 以前在 Doctor 上是绿的。检查方式按采集方式分：整链的名字（27 个 resident + 基准）比覆盖率，窗口的名字（543 个 core/edge，只取近价 3 个到期 ±15% 行权价）比"有没有写进来"。仍是 ⚠️：`_slot_adherence`（`api/ingest_dashboard.py:513-711`）仍以 job 证据判定 `on_plan/missed`，未对数据判定。 |
| C-C1 | ✅ | 发布节奏在契约里声明（`DatasetContract.cadence`：`session` / `settlement`），只有 `session` 的才拿交易日历判缺失。实测依据：最近 120 天的中位间隔，`short_interest` 15 天（双月结算），`ratios` / `short_volume` / `stock_daily` / `treasury_yield` 各 1 天。第一次读数时没声明节奏，`short_interest` 被误报 56 天缺失。 |
| C-C2 | ✅ | 缺失日与稀薄日分列，`days_absent` / `days_thin` 两个字段，Console 的 Continuity 列写成「1 missing · 1 thin of 25」。 |
| C-C3 | ✅ | 稀薄对**前序产出**判定（`continuity.thin_days`，前 10 天中位数的 50%）。试错三次：全窗口中位数把 09-08 宇宙 26→575 的台阶判成 25 个洞；居中邻域仍误判台阶两侧；前序才对。实测 `option_open_interest` 从 25 个误报降到 8 个真的，同时保住 `option_daily` 2026-07-18 只有 1 行（此前 45,534）。 |
| C-C4 | ✅ | 读取时计算（`/market/coverage/dimensions` 第四轴），不依赖事后记录——记录只能看见打开之后。编译耗时 94 → 155 秒，`short_volume` 回填到 700 万行后升到 286 秒，都在后台缓存后面。 |
| C-G1 | ⚠️ | `DataInventoryStrip` 的三个假分母已修：Option/Snapshots 用过 `max(watchlist, 实际值)`（不可能小于被测量的数），Stock Day 是"大于零即满格"的存在标志画成满条。现在都除以契约表的分母，且**没有分母时不画条**而是明说。**更正 2026-09-09.2 的一处误判**：`OptionCoverageSection.tsx:191` 的 `max(1, ...)` 是条形图的相对刻度（`contractCount / maxContracts`），不是覆盖率分母，不该被列为假分母。本轮逐一核对了 Console 的 market-data 面板：`ScoreRing total=` 的十来处 `Math.max(x.length, 1)` 都是分区总数，不是覆盖率分母。**查出并修好第五处真的假分母**：`analyticsDemandModel.ts` 的 `optionTarget = max(watchlist, snapshot, oi, 1)`（分母把被测量的数算了进去，采到 1 个也是满格）、`inputOf` 的 `target = count` 默认值（分母就是分子）、`CS_FUND_TARGET = 5000` 手写常数、`Stock daily` 的"大于零即 100%"——四处全部改为除以 `/market/coverage/dimensions` 的契约分母，**没有分母就不画条**。它同时喂着 Overview 页与 `massiveAgentPack`，是这一类里影响面最大的一处。**同一个错误在同一处又犯了一次并当场修掉**：把 Stock daily 的表指向全市场分母时，
分子取的是 `stock_daily` 有史以来见过的 20,695 个标的，分母是今天活跃的 5,317 个 ticker，卡片上写着"20,695 / 5,317"（不夹逼就是 389%，正是四条契约首读时的同一个毛病）。
改成两端都取自 `/market/coverage/dimensions` 已经算好的同源读数：持有 5,182 / 口径 5,317，范围外 7,336。教训是同一条：**比率的分子必须取自它自己的分母集合**。 |
| C-G2 | ⚠️ | 19 个数据集全部报出三个轴（`/market/coverage/dimensions`）。仍是 ⚠️：`stock_movers` 是 top-N 榜单，用全市场当分母得到 0.4%，是无意义的比率——它需要自己的档位。**更正一处旧读数**：此前记的"分钟线只覆盖 11 个基准里的 3 个"是分母本身错了（见 C-B1 的 benchmark 档），真实读数是 18/26（`stock_minute`）与 8/26（`option_minute`），且 `outside_scope` 归零。 |
| C-G3 | ❌ | entitled 但未持有的 8 个数据面（§4）在任何界面上都不可见；`/market/capabilities` 只列 planned（需升级）与 unavailable（端点 404），不列"已付费但没在采"。 |
| C-G4 | ✅ | 0.30.0 把四个 verdict 函数从 `dimensionsModel.ts` 移进插件的 `verdicts.py`，payload 每行带 `verdicts`；Console 只在 payload 没有该字段时（比 Plugin 先发布的窗口）才走本地回退，两侧用同一批用例互相钉住（`tests/test_verdicts.py` ↔ `dimensionsContinuity.test.ts` / `coverageMatrixMemory.test.ts`）。 |
| C-G5 | ✅ | `ops_jobs.coverage_sample` 记录每次 compute 的 76 个判定，**按变化游程压缩**：判定没变就只更新 `last_seen_at`，变了才写一行。矛盾的化解写在 §2i。 |

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

## 2c. 厚度首次读数（2026-09-09）

窗口 120 天。**缺失**对交易日历（仅 `session` 节奏），**稀薄**对前 10 天的产出中位数。

| 数据集 | 节奏 | 有数据的天 | 缺失 | 稀薄 | 最薄的一天 |
|---|---|---|---|---|---|
| `stock_daily` | session | 82 | 0 | 0 | — |
| `ratios` | session | 23 | 0 | 0 | — |
| `short_volume` | session | 81 | 0 | 0 | — （补完后；补之前是 12 在场 / 68 缺失） |
| `short_interest` | settlement | 7 | 0 | 0 | — |
| `treasury_yield` | session | 80 | 0 | 0 | — |
| `option_snapshot` | session | 24 | 1 | 1 | 2026-08-18：9,716（此前约 22,480） |
| `option_open_interest` | session | 56 | 0 | 8 | 2026-07-24：384（此前约 2,027） |
| `option_daily` | session | 81 | 0 | 4 | 2026-07-18：**1**（此前约 45,534） |
| `stock_minute` | session | 22 | 1 | 0 | — |
| `option_minute` | session | 22 | 1 | 5 | 2026-09-03：8（此前约 180） |

**为什么需要这一轴**：同一时刻 `stock_daily` 的另外三轴是 20,695 个标的、五年历史、当日新鲜——全绿。而最近 90 天里有 **7 个普通交易日只有 15–19 行**（正常 12,400）：`universe-daily` 的全市场调用失败、`stock-eod` 的自选股照跑。doctor 当天必然报了 crit，但它是**无记忆的逐 session 检查**，那天过去洞就永久隐形。这七天已用 7 个 `universe-daily --date` 任务补回，上表的 0 是补完之后的读数。

**最大的一处是 `short_volume`，已补完**。这一处比首读时看到的更糟也更好修：全表只有 17 个日期，其中 15 个各只有 1 行（退役的逐标的路径残留），真正全市场的只有两天。
契约把它标成 `forward_only`（只能向前累积）是**从 `ratios` 抄来的、没验证过**——`ratios` 的端点确实忽略 `?date` 只返回最新，而短量不是：实测 `?date=2026-06-15` 返回的就是 6 月 15 日的行，窗口约 2 年（2024-09-16 有、2023-09-15 空）。契约已改为 `rolling_days` 730 天，496 个交易日已回填，**501/501 全部齐了**，700 万行 / 4.6 GB，0 失败。

回填只发 `short_volume_market` 单个 kind：走 `--slot fundamentals-market` 会连带发 `ratios_market`（日期被忽略，纯浪费）和 `short_interest_market`（结算频率，45 天回看已覆盖）。优先级给 4，因为 `fundamentals-market` 默认的 2 和期权回填同级，按创建时间排永远排不上。

**这一轴的已知盲区**：前序基线只能发现「一直好好的然后掉下去」，发现不了「从来就没好过」。那 15 个各 1 行的日子是均匀的坏，没有健康邻居可比；广度也看不见它，因为广度数的是「有史以来见过的标的」，两个好日子就贡献了 5,216 个。补法可能是给全市场日频数据集在契约里声明每 session 的行数下限（类比 doctor 的 `STOCK_DAILY_MIN_SESSION_SYMBOLS`），未做。

## 2d. 本轮收敛（2026-09-09，Plugin 0.19.0 / 0.19.1）

**四套 session 定义 → 一套。** `session.py` 持有唯一定义与截止时间算术，从**收盘**起算而不是从午夜——一个 22:00 落库的数据集不该在次日早上显示成快一天旧。迟到需要同时满足两件事：**截止时间已过**，且**收盘后没有任何一次运行**。于是"周五晚的数据在周一早上"自然是待定而非陈旧，旧规则为此专门开的 72 小时周末例外**不再需要存在**。

**四套阈值 → 契约表。** doctor 的 `STALENESS` 改为从 `contracts.staleness_by_slot()` 派生，一条测试断言两者逐条一致。**巡检范围仍由 doctor 显式声明**（`POLICED_SLOTS`）——派生表覆盖 16 个 slot，直接放开会把 verdict 从 healthy 变成 degraded，那是"什么该告警"的决定，不该夹带在收敛里。`corporate` 保持 168 小时：分红拆股本就稀疏，48 小时会去告警日历而不是告警数据源。

**顺带修好一个既有脆弱点。** quality gate 的 `count(DISTINCT symbol) FROM stock_daily` 在回填负载下**超过 10 分钟**，整个 gate 返回 500。`stock_daily` 按年分区，跨分区 DISTINCT 无法提前收敛，加 `LIMIT` 也没用。改为问 doctor 一直在问的那个问题——**当期 session 的宽度**（单分区等值扫描，10 秒），读数 12,518 个标的对 4,000 的下限，verdict 不变，但在回填期间够得着了。

线上验证（0.19.1）：doctor 19.8 秒五条 staleness 全 ok；quality-score 从 500 变为 23 秒，freshness 按维度给出各自截止时间（2h / 2h / 2h / 48h），平铺阈值为 `None`。

## 2e. 队列跑空后的四轴普查（2026-09-10，Plugin 0.21.0 / 0.21.1）

2.4M 个回填任务全部跑完、队列归零，是读四轴最干净的时刻——没有在途任务干扰读数。19 个契约逐个读下来，结论是**真正缺数据的只有 1 处，需要改代码的 2 处，量错了的 4 处**。

### 真的缺数据

**两个周二整天缺席**，且在多个数据集上对齐：2026-08-11（`option_snapshot` / `stock_minute` / `option_minute`）与 2026-09-08（`option_daily` / `stock_minute` / `option_minute`）。同一天在三张表同时消失是排程中断的形状，不是采集失败的形状。minute 与 `option_daily` 已补（`minute-bars` 两天各 134 个 job；`option-bars` 09-08 一天 **69,950** 个 job）。`option_snapshot` **补不了**——链快照只反映当前 session，这是订阅性质，不是缺陷。

`treasury_yield` 落后 6 天，一个 30 天回看的 `treasury_yields` job 补回（20 行）。为什么 09-08 / 09-09 两天没自动出数，未查。

### 代码缺陷

**调整后期权合约被解析器整类拒收。** OCC 在拆股、并购、特别股息之后会给根代码追加数字后缀，而 ticker 正则只允许字母、点、连字符，于是 `O:WDC1250221C00005000` 直接抛错。24 小时内 3,861 个 `option_daily` job 因此失败，抽样 481 条**全部**是这个原因，涉及 `WDC1` / `XOM1` / `XOM2` / `CVX1` / `SW1` / `SPGI1`，全部对应真实公司行为。

决定性证据是**目录和采集互相矛盾**：`option_contract` 收下了这些合约（SPGI1 910 张、WDC1 106 张、CVX1 64 张），只有 bars 路径拒收。`option_snapshot` / `option_open_interest` 把解析器当兜底用（vendor 在这两个端点返回合约明细），失败只丢一行；`option_daily` / `option_minute` 在发 HTTP 前无条件解析，一失败整个 job 死，那张合约的日线历史归零。

Doctor 给的 `retry-jobs` 处方在这件事上**是无效的**——不改正则，3,298 个「可重试」job 重跑还是全挂。修法是让根代码接受数字，并从右侧定长的 15 字符尾部（6 位日期 + 1 位方向 + 8 位行权价）取切分点，因此没有歧义。0.21.0 实测：`O:SPGI1260918C00240000` 从抛错变为 `done`，`underlying` 正确解出 `SPGI1`。

**`option_daily` 的两年历史，本来从今天起会开始烂掉。** 深度读 503/575 达标（回填成功，中位 731 天），广度读 25/575——因为 `option-bars` 的范围是 watchlist ∪ 基准，不是 575 的研究宇宙。那 550 个标的的历史会停在回填结束那天不再前进，而**没有任何一轴看得见**：25 个标的每天都写行，per-day 计数一直健康。

厚度轴这次其实**看见了**边缘：最近 4 个交易日逐日走薄，08-31 (21,357) → 09-03 (15,862) → 09-04 (14,860) → 09-09 (**2,714**)，而更早的邻居中位数是 27,907–45,628。回填留下的健康基线让这个台阶显了形——上一轮记的「均匀的坏」盲区这次没生效，因为坏是从某天开始的。

Owner 定：**扩范围**。`option-bars` 改吃 `research.option_universe`（watchlist 仍是空表时的兜底）。实测代价 **69,950 job/交易日**，约为原维护预算（~5,700/交易日）的 12 倍。

### 量错了，不是数据坏了

| # | 读数 | 真相 | 处置 |
|---|---|---|---|
| 1 | `short_volume` 广度 0/5,317 | 端点自报 `statement timeout`。当天实有 15,248 行 | 这几张表的索引全部以 `symbol` 打头，「最新一天持有谁」只能全表扫。76k 行时无所谓，我上一轮把它养到 700 万行 / 4.6 GB 就爆了 120 秒预算。加 `(period_date, symbol)` 走 index-only |
| 2 | 厚度轴的稀薄日 | `option_open_interest` 最严重的 5 天有 4 天是**周六/周日** | `days_absent` 过滤了交易日历，`days_thin` 没有。非交易日现在同时退出被判序列和它的基线；落在非交易日的行改报 `days_off_calendar`——那是洞的反面，此前无人可见 |
| 3 | `short_interest` 落后 27 天 | **根本没落后**。2026-08-14 就是 FINRA 已发布的最新一期，库里 08-14 / 07-31 / 07-15 / 06-30 / 06-15 一个不缺 | 拿 30 小时的 deadline 去量双月结算的数据集。`cadence` 字段早就在契约上，新鲜度轴从没读过它。改为按数据集**实测**的发布间隔判定，超过两个间隔才算迟到 |
| 4 | `option_snapshot` / `option_open_interest` 深度 0/570 | 目标写 90 个 session，但 EOD 链快照**无法回填**；中位 2 天是因为全宇宙覆盖刚铺开 | 与 C-D3 同类：计划边界不该显示为缺口。**未改** |

第 5 处一并记下但未改：三张财报表 `at_target 0/4,467` 是目标定义问题——「2009 年起」换算成 6,461 天，最深的标的 6,404 天（2009-02），没有任何标的能达标，因为大多数公司那时还没上市。有意义的读数是深度中位数 3,540 天（≈2016）。

### 顺带发现

**schema 迁移 Job 从 0.11.1 起就没升过级。** kustomize 的镜像替换按镜像名匹配，而只有这个 Job 把 registry 路径写全了，`newTag` 因此够不到它。十个版本的 DDL 从没走过这条路——它 2026-09-10 的日志里列出的 `ops_jobs` 表仍是 0.11.1 那批，`queue_sample`（0.20.x）与 `treasury_yield` 都不在。改成裸名后重跑，两者都出现了。

**周末有行。** `option_open_interest` 在 08-22 / 08-23 / 08-30 / 09-05 有几千行，而 `option-bars` 与 `eod-pipeline` 都在跳过休市日的名单里。来源未查。

**`stock_minute` / `option_minute` 的分母有 8 个标的没人跑**：`minute-bars` 实测覆盖 18 个，benchmark 档的分母是 26。

## 2f. 排程能防止腐败吗（2026-09-10，Plugin 0.22.0）

回填跑完后逐 slot 核对「续期范围 vs 契约分母」，答案分三层。

**广度**：`option-bars` 改吃全宇宙之后，每个数据集的续期 slot 都覆盖到了它声明的分母，只剩两处例外——`minute-bars` 用 watchlist（18 个）而 benchmark 档的分母是 26，**那 8 个标的永远轮不到，不会自愈**；`option-refresh` 是 batch 12 × 每 6 小时的轮转，575 个标的转一圈约 12 天（设计如此，但目录会滞后）。

**深度**：真正的风险已消除。回填给 575 个标的买了两年历史而续期只覆盖 25 个——那 550 个的历史会停在回填结束当天。改成 `universe: research` 后实测 575 标的 / **69,950 job/交易日**。

**厚度：原本没有任何自动保障，本轮补上了。**

大多数 slot 只填当天，漏一次点火就是永久的洞。只有四个带回看窗口能自愈：`treasury`(30 天)、`corporate`(7 天)、`short_interest`(45 天)、`fundamentals-rotate`（每天跑全池）。而结构上有一道断裂：

- **厚度轴能看见**旧洞（120 天窗口），但它是只读度量，不产生处方
- **Doctor 能开处方并执行**，作用域却是 `resolve_session()` 的**一个 session** + 24 小时失败窗口
- 每晚 00:45 的 Dagster `market_self_heal` 调的就是 `GET /market/doctor`，继承了同一个作用域

**能发现洞的修不了，能修的发现不了。** 2026-08-11 那个洞躺了一个月正是因为这个——它发生当天 doctor 一定报了 crit，但 doctor 没有记忆，第二天就永久隐形。

0.22.0 把两者接上：doctor 新增 `continuity:<dataset>[:<date>]` 一族 finding，在 60 天窗口内找出「日历上是交易日、表里没有任何行」的 session，并给出**精确到那一天**的 `enqueue-slot` 处方。`market_self_heal` 不用改一行就获得了修补旧洞的能力。

三条刻意的边界：

1. **能不能补是契约声明的，不是推断的。** `DatasetContract.backfill_slot` 为 None 表示这一天**找不回来了**，而不是「还没接线」：EOD 链下载只返回当前 session（`option_snapshot` 与派生的 `option_open_interest`），`ratios` 的端点忽略 `?date` 只能向前累积。给这些开处方是撒谎，不是修复。目前 19 个数据集里 5 个可补。
2. **每次每个数据集最多开 3 张处方**，且当上限生效时会在 detail 里明说还剩几天。一个 `option-bars` 日 ≈ 7 万个 job，无上限的处方遇到坏了一个月的数据集会在一夜之间压进去几百万。
3. **不进 `EOD_CRITICAL_CHECKS`。** finding id 前缀是 `continuity:`，不会阻塞 Research 的 dbt 批次——一个可补的旧洞是 warn，不是 crit。

### Coverage 一屏：宏观与细节分层（0.24.0 + Console）

Owner 的两点观察，实测都成立。

**这一屏在读者表达任何兴趣之前先付了约 52 秒。** 挂载时并发打 11 个端点，逐个计时（经 platform-api 代理，2026-09-10）：

| 端点 | 耗时 | 性质 |
|---|---|---|
| `coverage/inventory` | 7 ms | 宏观（已缓存） |
| `coverage/dimensions` | 9 ms | 宏观（已缓存） |
| `ingest/queue-dashboard` | 145 ms | — |
| `coverage/sepa-stats` | 201 ms | 细节（便宜） |
| `coverage/watchlist` | 7.5 s | 细节 |
| `snapshot-quality-detail` | 11.0 s | 细节（单标的 14 天） |
| `coverage/quality-score` | 12.0 s | **宏观** |
| `coverage/db-summary` | 16.4 s | 细节 |
| `coverage/contracts?limit=500` | 21.0 s | 细节 |
| `coverage/greeks?limit=500` | **52.5 s** | 细节 |

四轴那份 14.5 KB 的数据 **9 毫秒**就回来了。慢的部分全部是细节，而唯一慢的宏观读数是那个 4/4 判定。

**折叠不停查询。** `OpsSection` 用的是 `<details>`——折叠只隐藏、不卸载，子组件的 `useQuery` 照跑、照着 60 秒 `refetchInterval` 反复重打。所以「默认收起」这条设计规则在实现层从来没有生效过。`OpsSection` 现在会报告展开状态，三个重面板默认收起并把查询挂在上面；`db-summary` 与 `watchlist` 从 tab 顶层移下去。

**结果：11 个端点降到 5 个，首屏从被 52 秒封顶变成被 200 毫秒封顶。** 展开时才取，已实测。

**矩阵（契约新增 `grain`）。** 档位说「哪些标的」，粒度说「一行是什么」，两者互不可推：`option_snapshot` 与 `option_daily` 同档不同粒度，`stock_daily` 与 `option_daily` 同粒度不同档。所以 `grain` 是声明的，和 `cadence`、`backfill_slot` 一样——Console 里手写一份映射，第一次加数据集就会漂移。

矩阵把 19 个契约排成档位 × 粒度，每个数据集带同样四个标记（B/D/F/C），颜色就是四轴表给出的那个判定——**不新增任何判断逻辑**，所以扫格子的人和读表的人不可能被告知不同的事。空格保留：「基准之外没有分钟数据」是一件应该不用特意去找就能看见的事。

**顺带修掉两个会误导读者的判定**：

1. **「还在算」曾被渲染成 PASS。** `quality-score` 挪到后台缓存后，首答是 `{ok: true, summary: null, checks: []}`，而 Console 那行 `score?.summary ?? (score?.ok === true ? 'PASS' : …)` 把它读成了通过——在一次**尚未发生**的检查上刷绿。这正是第四轴存在的理由那个形状（跳过被记成成功）。判定移进 `qualityScoreModel`，没有答案时返回 null。
2. **没有时钟的数据集曾显示成灰色 unknown。** `ticker` / `option_contract` / `us_market_holiday` 根本没有日期列——它们列举存在的东西，不观测它。深度轴一直把这叫 boundary，新鲜度轴叫的是 unknown。改齐之后矩阵从 2/19 clean 变成 5/19。

**这两个的深度已改为计划边界（0.24.2，Owner 决定）**：`option_snapshot` 与 `option_open_interest` 原本声明 `sessions/90`（trim 保留量），于是量出 0/570 达标、中位 2 天，渲染成红色。但 EOD 链下载只返回当前 session——2026-08-11 之所以永久缺失正是这个原因——所以这个深度只能向前累积、买不到。红色是把爬坡报成了缺陷，而这块矩阵的目的恰恰是降低理解门槛。

`forward_only` 本来就是这个语义且已在 BOUNDARY_KINDS 里，90 这个数移进 `why`，trim 保留多少仍然说得出来。

**代价说清楚**：失去的是「每标的深度中位数」——爬坡进度。但这两张表真正该被判定的是「每个 session 链有没有落地」，而**厚度轴仍然在量它们**（`forward_only` 是 continuity kind）：实测 `option_snapshot` 25 在场/1 缺失/1 稀薄，`option_open_interest` 48/0/1+9 日历外。附带好处是深度轴不再为这两张表付一次 570 个标的的扫描——那次扫描只是为了跑完再丢掉。

### 广度曾把盘中分子除以全宇宙分母（0.25.0）

`option_snapshot` 在 2026-09-10 同一天读出 99.1% 和 4.5%，数据没变。两个 slot 写这张表：`eod-pipeline` 22:00 UTC 写全部 575 个，`intraday-chain` 14:30 UTC 写 26 个基准。广度读的是 `max(date)`，所以从 14:30 到次日 EOD 之间，它拿**盘中链的分子**除以**全宇宙的分母**——每天七个半小时的假红，正是 C-B1 要防的那件事。

改为按 `session.py` 解析出的 session 界定（C-F1 那套唯一定义，这个读数从来没用过它）。**是界定，不是过滤**：`max(date <= session)` 而非 `date = session`。差别不是细节——落后两天的 `treasury_yield` 若按 session 过滤会读成 0/1，用广度去重复报新鲜度已经在报的事。解析不出 session 时回退旧读法，而不是静默报告空。

### 「没有补法」曾有三种含义（0.26.0）

0.22.0 加的 `backfill_slot` 用一个 `None` 编码了三件事：vendor 不会再给那一天、slot 自己的回看窗口会修好、这里根本没有 session。Console 的 Agent 简报对三者都按第一种读，于是告诉读者 `treasury_yield` 的缺失 session「gone for good」——而那个 slot 每次运行重拉 30 天。`corporate_action` 同理，7 天。

改为显式声明的 `Refill`（形状对齐 `DepthTarget`）：

| how | 含义 | 数量 |
|---|---|---|
| `slot` | 发这个 slot + 日期 | 4 |
| `kind` | 发单个 job kind——整个 slot 会多做事 | 1 |
| `lookback` | 什么都不用做，slot 自己的窗口会修 | 9 |
| `unrecoverable` | vendor 不会再给那一天 | 5 |

`short_volume` 归入 `kind`：它的 slot 会连带发 `ratios_market`（端点忽略日期）和 `short_interest_market`（45 天回看已覆盖）——每修一天浪费两个 job，2026-09-09 量过并记在本文档里，而简报当时没带上。Doctor 现在开单 kind 处方，heal 为此新增 `enqueue` 动作（插件本来就能做，只是处方词汇表达不出来）。自愈型数据集**不再开处方**：开了就是让每晚的 self-heal 去补一个排程自己会关的洞。

## 2g. 矩阵照出来的问题：红色目前不等于「要修」（2026-09-10）

矩阵上线后第一次逐条核实那 14 条 not clean，**只有约 4 条是真实数据缺口**，其余是度量本身错了。分五类：

| 类别 | 数据集 | 问题 |
|---|---|---|
| **分母错** | `stock_movers` 22/5,317 | top-N 榜单，分母应是榜单容量（C-G2 早标 ⚠️） |
| | `corporate_action` 756/5,317 | 不是每个 ticker 都有公司行为 |
| | `ratios` 3,975/5,317 | vendor 只对约 5,000 个 ticker 算比率——这条在 program skill 里写明是「不是 bug」 |
| | 三张财报 ~83% | ETF / 信托没有财报 |
| **目标不可达** | 三张财报 `0/4,467` | 「2009 起」对后上市的公司不可能达标 |
| | `stock_daily` 10,862/20,703 | 分母含已退市标的，它们不可能有「截至今天的五年」 |
| | `short_volume` 13,835/29,682 | 同上 |
| **节奏错** | 三张财报 `+39d` | 拿 48 小时 deadline 量季度数据。**与 `short_interest` 是同一个 bug，那次修漏了它们** |
| **爬坡当缺陷** | `stock_minute` / `option_minute` `0/18` | 目标 365 天、实际中位 36 天，数据 36 天前才开始收 |
| **轮转口径** | `option_minute` 广度 3/26 | batch 80 的轮转每天只覆盖约 3 个标的，按「本 session」量必然低 |

**这直接影响 Ask Agent 的可信度**：简报继承同一批读数，会把 Agent 送去修不存在的问题。矩阵做对了它该做的事——把这些一次性摆到同一屏，让它们无法再各自躲着。

## 2h. 让红色重新意味着红色（0.27.0 – 0.28.0）

§2g 记下矩阵照出的五类度量偏差，这一节记它们怎么修的，以及修的过程中被数据否定的一个假设。

### 五步

**1 · 财报的节奏（0.27.0）。** `cadence` 新增 `filing`。公司什么时候报就什么时候报，而 `period_date` 跟的是每家自己的财年，所以「最新一行」只说明谁最近报过。拿 48 小时的 deadline 去量，三张表读出 `+39d` 而一切正常——**和 `short_interest` 是同一个 bug，那次修漏了它们**。新鲜度现在返回 `judged: false` 并给出理由，而不是红色。

**2 · 深度的 population（0.27.0）。** 深度数的是「这张表有史以来见过的每一个标的」（`stock_daily` 20,703），而广度除的是档位口径（5,317）。**同一个数据集、两个 population**——C-B1 要终结的正是这件事，这次落在另一个轴上。那条长尾大多已退市，「截至今天的五年」从来就不是对它们的要求。改用同一个 population 之后：

| | 修前 | 修后 |
|---|---|---|
| `stock_daily` | 10,862 / 20,703 | 3,371 / 5,316 |
| `short_volume` | 13,835 / 29,682 | 4,188 / 5,316 |
| `option_daily` | 503 / 576 | 501 / 571 |

**3 · 绝对起点只报分布（0.27.0）。** 2020 年上市的公司永远够不到 2009。`income_statement` 的 4,467 个标的全部「未达标」，而中位数握着 9.7 年——那是把一个「什么都没数到」的计数渲染成了缺陷。`since` 类目标现在报中位/最深/最浅，不算通过率。

**4 · 广度不问不该问的（0.27.0 + 0.28.0）。** 三个数据集的广度不再被当作覆盖率：

- `stock_movers` —— 当日涨跌幅 top-N 榜单。22 个就是整张榜，不是全市场的 0.4%
- `corporate_action` —— 发生过的事件，不是待覆盖的标的。窗口内大部分 ticker 没有拆股或分红
- `ratios` —— 见下

**5 · 轮转不按单 session 量（0.27.0）。** `minute-bars` 从 watchlist 的链里取 80 张近价合约，每天触及约 3 个标的、约十天覆盖全部。按「本 session」量，`option_minute` 读出 3/26 并因为**正常工作**而变红。改为 `ever` 之后是 18/26。

**第 4 步我原本还列了一条「爬坡当缺陷」，核实后撤回：** `stock_minute` 的 36 天对 365 天目标**不是**爬坡。minute 聚合接 from/to，那份深度**买得到而没买**。红色在那里是真的，不该被开脱。

### 被数据否定的假设（0.27.1）

三张财报读出 ~83%，我猜是 ETF 和信托把分母撑大了，于是给它们加了 `common-stock` 档位。部署后一量：`common-stock 5,317`，`whole-market 5,317`，**完全相同**。

`raw_market.ticker` 里只有活跃美股普通股——5,317 行**全部**是 `instrument_type='CS'` / `market='stocks'`，因为参考同步只拉这个。分母本来就是对的，而那个档位是 whole-market 的一份副本，还暗示了一个并不存在的区分——正是这一轮在修的同一类错误。整个撤掉。

83% 是**真读数**，琥珀色是对的颜色：vendor 对另外 900 个没有财报。

### `ratios` 的 1,000 个缺口：两个重叠的集合，不是缺口

`ratios_market` 每次 **6 页、5,010 行、`truncated: False`**——vendor 给多少收多少。分解：

| | 数量 |
|---|---|
| 本 session 落在档位内 | 3,975 |
| 本 session 持有总数 | 4,791 |
| **vendor 算了、不在我们活跃列表里** | **816** |
| **我们的活跃标的、vendor 不给算** | **1,342** |

对照同一个 slot、同一份订阅的 `short_volume`：持有总数 15,248、档位外 10,014、档位内 5,234 = 98.4%。所以不是采集窄，是 **vendor 对 ratios 的覆盖面本来就窄**——program skill 从 2026-09-06 起就写着这句：shortfall 是微盘股和新上市，**不是 bug**。

### 档位的定义（0.28.0）

Owner 读到「Whole market 5,317」问的是：这是什么？Stock？SEPA 的 stock？读到「Universe 575」问的是：市值 > 2 亿的？

**两次都猜得方向对、量纲错**，而这比不知道更糟。定义原本只存在于代码注释里，UI 上一个字都没有：

| 档位 | 实际是什么 |
|---|---|
| Active US common stock 5,317 | `raw_market.ticker WHERE active`——vendor 列为活跃的美股普通股，`reference` slot 的产出。**没有任何筛选**：不按规模、不按流动性、不是 SEPA |
| Option universe 575 | `research.option_universe`，**Research 的规则**。resident = 持仓 + watchlist + IV 基准，永不移出；core = **20 个 session 平均成交额 ≥ $2 亿**入选、跌破 $1.2 亿才出；edge = SEPA SETUP/PIVOT 评分 ≥ 70。**是成交额，不是市值** |
| Watchlist + IV benchmarks 26 | 自选股 ∪ IV 雷达基准。这一档存在是因为盘中数据太大，只值得为少数几个收 |
| Single series 1 | 一条序列，不是每个标的一条——国债曲线、交易日历 |

声明在 `scopes.py` 里紧挨着 scope 本身，随分母一起进载荷，Console 只渲染不重写——数字和它的含义不会各走各的。Agent 简报也加了这一节：**一个读到「Universe 575」而没有定义的 Agent，会和人猜错同一个方向。**

### 结果

`5/19 clean` → **`8/19`**，`Ask agent — 14 to fix` → **11**。剩下的红大多是真的：`option_daily` 广度 25/575（今晚 22:45 的 cron 才是扩范围后第一次）、minute 深度 36 天对 365 天（可填未填）、几处 continuity 的稀薄与缺失日。

### 期权目录的轮转（0.23.0）

`option-refresh` 名义上是「每 6 小时一批 12 个」，看上去 575 个标的 12 天转一圈。**实测是约 48 天**，因为轮转偏移是 `sha256(目标日期)`——一天之内四次运行算出同一个偏移、取同一批：

```
09-09 18:20  SITM, SLB, SMCI, SMR, SMTC, SN, SNDK, SNOW, SNPS, SO, SOFI, SPG
09-10 00:20  SITM, SLB, SMCI, SMR, SMTC, SN, SNDK, SNOW, SNPS, SO, SOFI, SPG   ← 同一批
09-10 06:20  ROST, RRX, RSG, RTX, RVMD, RY, SBUX, SCCO, SCHW, SGI, SHOP, SHW
09-10 12:20  ROST, RRX, RSG, RTX, RVMD, RY, SBUX, SCCO, SCHW, SGI, SHOP, SHW   ← 同一批
```

去重只挡 pending/running，不挡已完成的，所以重复的三次是真的重新拉了一遍 vendor。

**为什么这件事要紧**：`option-bars` 的近价合约是**从 `option_contract` 里选的**。目录滞后，新挂牌的近月/周度到期就进不了日线采集的候选池。这两个 slot 之间存在方向明确的依赖，而上游的实际刷新率比标称慢四倍。

单纯提 `batch_size` 不解决问题——四次运行还是同一批，只会变成四份重复。所以轮转依据换成**「谁的目录等得最久就先刷谁」**（`option_contract.updated_at` 的每标的最大值，靠新增的 `(underlying, updated_at DESC)` 索引做索引探测）。它每次运行都推进、不需要时钟、漏掉一次能自己补回来；读不出来时退回原来的日期轮转，而不是把所有名字当成同样陈旧（那正是 2026-09-08 那次「每次都从 A 开头重枚举」的形状）。

`batch_size` 12 → 144：4 × 144 = 576，覆盖约 564 个非基准名字，**一天一圈**。

实测代价（97 个 job 的样本：中位 4.2s / 8 页 / 1,922 行，p90 23.8s，最大 78.6s / 115 页 / 28,746 行）：

| | 改前 | 改后 |
|---|---|---|
| job/天 | 92 | 620 |
| worker 时间/天 | ~13 分钟 | ~1.5 小时 |
| vendor 页/天 | ~1,600 | ~10,500 |
| 一圈 | ~48 天 | **1 天** |

一条不免费的代价：每次采集都对触及的行 `SET updated_at = now()`，是全行重写。日行更新量从约 37 万涨到约 130 万，在一张 73 万行的表上是每天约两倍的行周转。autovacuum 够用，但记在这里。

### 这条检查的查询形状（0.22.1 → 0.22.3 的三次修正）

值得记下来，因为每一步都「看起来对」而实测不对：

1. **复用 `per_day_counts`**（为稀薄日统计写的，必须计数）→ doctor 中位数 9s → 19s。用聚合回答存在性问题。
2. **改成逐日 `LIMIT 1` 探测** → 中位数 17s，几乎没动。每条更便宜，但 42 天 × 5 个数据集 = **210 次网络往返**，成本从「工作量」变成了「延迟」。
3. **整份日历用 `VALUES` 送到服务端，一次往返** → `option_daily` 的读取**超时被跳过**，findings 里少了一条，而它正是这条检查存在的理由。原因是 `NOT EXISTS` 配一个四十行的外表，规划器会读成「把内表整个哈希掉」。
4. **`LEFT JOIN LATERAL … LIMIT 1`** → 必须逐日求值且遇到第一行就停。五个数据集全部回来。

教训有两条。一是**守卫做对了事反而掩盖了问题**：读取失败时跳过而不是猜，这是对的，但结果是检查静默地漏掉了最重要的表——所以「findings 少了一条」必须被当成信号读。二是**便宜的查询和便宜的往返是两件事**。

### 调整后合约归一（同版）

0.21.0 让解析器接受了调整后合约，但没有告诉它这些合约属于谁：`option_daily` 用**解析出来的**根（`BDX1`），而 `option_snapshot` 一直用请求里的 `storage`（`BDX`）。结果是深度轴数出 581 个标的而快照表数出 570，且下游 `WHERE underlying = 'BDX'` 会整条漏掉调整后序列。这些行在 0.21.0 之前不可能存在——那时 job 直接失败。

修法是 enqueuer 在 payload 里带上**目录表的** underlying（它本来就是按 underlying 选的合约），handler 优先用它。历史行由一条幂等迁移订正，依据同样是目录表：逐个 `(root, canonical)` 走 `(underlying, bar_date)` 索引更新，并用 `EXISTS` 把重写绑定到**那一张具体合约**上——所以一个碰巧长得像调整根的真实代码不会被误改。

## 2i. 给矩阵一份记忆（0.29.0 – 0.30.0）

### 边界也带累积进度（0.29.0）

矩阵上 76 个判定里有 31 个是 `boundary`，其中 13 个在深度轴上。Owner 的问题是对的：**这么多蓝色，那这个矩阵的能力是不是有待提高**。蓝色本身没错——「计划边界」不是缺陷——但它把两种完全不同的状态画成了一个样子：`option_snapshot` 的深度是「链下载只返回当前 session，所以只能一天天攒」，它**正在往 90 个 session 爬**；而 `ticker` 的深度是「目录表就没有历史」，它哪儿也不去。

修法不是再加一种颜色。空心标记的内部按累积比例填一段**边界自己的颜色**（info，不参与严重度排序），所以「在爬」是形状上的差别而不是等级上的差别。两把尺子还是两把：颜色排等级，填充说有没有判定。

### 静止画面说不出「变差了」（0.30.0）

真正的能力缺口在别处。矩阵是一张**静止画面**：它能说 `option_daily` 广度现在是 thin，说不出它是**昨晚才变成** thin 的。对一个 Agent 来说这两句话的差别是「这是已知状态」和「你昨天改的东西弄坏了它」。具体到眼下：今晚 22:45 的 option-bars 是扩到 575 个标的后的第一次运行，**如果它回退了，明天的矩阵和今天长得一模一样**。

先决条件是判定得先搬到能被记录的地方。四个 verdict 函数原本住在 `dimensionsModel.ts`——阈值只存在于前端，等于没有第二个读者能看见它，doctor 用 Python 开处方，永远无法被告知一个判定退步了。0.30.0 把它们移进 `verdicts.py`，payload 每行带上 `verdicts`；Console 保留一份同规则的回退，只在 Console 比 Plugin 先发布的那段窗口里生效，两侧用同一批用例互钉（C-G4）。

### 一处必须先讲清楚的矛盾

当初为厚度轴主张的是**在读取时计算，不向前记录**——理由写在 `continuity.py` 的第一段：记录式的度量只能看见开关打开那天之后的事，而向后看正是它存在的理由。这里为什么反过来？

因为**判定无法向后计算**。新鲜度除的是「此刻这行有多旧」，广度除的是**当时那个档位口径**，深度比的是一个远端在移动的滚动窗口。明天重跑任何一个，回答的都是明天的问题。厚度不一样：它读的 session 现在还躺在表里，所以今天扫一遍就能说出七月的中间是什么样。

判据因此不是「哪个更好」，而是：**这个问题今天还能不能被重新问一遍**。能，就在读取时算；不能，就向前记录。写成 C-G5。

### 记录的形状

`ops_jobs.coverage_sample` 一行一次**变化**，不是一行一次 compute——页面按 TTL 反复重算，绝大多数重算逐字复现上一次的判定，把它们都存下来等于把「什么都没发生」存几百遍。判定没变就只更新 `last_seen_at`，变了才 INSERT。于是相邻两行天然不同，「什么时候变的」就是那一行自己的 `first_seen_at`，不需要去扫一串相同行的边界。

三个必须分开的状态（合成一个计数就等于又没有记忆了）：**没有可比对的上一次**（第一次读数）、**比对过且没有变化**、**记录写失败**。矩阵头部的标签、Agent 简报的 `## Since last reading` 一节各自把三者分开说。

**读失败不算一次读数。** 第一次真正落库的 compute 就撞上了这一条：`short_volume` 语句超时，四个轴全部返回 `unknown`——数据没动，只是有一条语句超了预算。照原样记下去，一个偶尔超时的数据集每翻一次就写两行，并且在一个专门用来让真回退显眼的页面上常年报「1 changed」。所以**读失败的数据集这一轮不贡献判定**，上一次的判定继续有效，`carried_forward` 里点名说它是「上次已知」而不是「刚测到」。只有**整条读失败**算数：`treasury_yield` 深度报 `unknown` 是因为它没有可摊开的 symbol 列，那是关于这个数据集的稳定事实，照实记。

**一个发布链的坑，顺手补上（0.30.1）。** 0.30.0 把建表写进了 `apply_ddl`——而集群从来不跑 `apply_ddl`，schema Job 是 `init_schema.py --wave8-only`。Job 自己打印的 `ops_jobs tables: … coverage_sample …` 是那个**声明用的元组**，不是 CREATE 跑过的证据。表不存在，API 每次写都答 `relation does not exist`，而记录路径老实地报成 `recorded: false` 而不是一个看起来像「什么都没变」的空 diff。建表语句改成一个函数、两个调用方（fresh install 与 deploy），并加了棘轮：`DATA_OPS_TABLES` 里的每张表都必须能被 deploy 路径建出来，除非显式列进 `FRESH_INSTALL_ONLY`。

保留 180 天，但**修剪永远保留最新一行**——安静了一个季度不该把「当前状态」的唯一描述删掉，随后每个数据集都报成第一次读数。留存约束的是历史，不是现在。

### 变化画在哪里

不加第四种颜色。矩阵已经有两把尺子（颜色=等级，填充=有没有判定），「这个动过」是一个关于**时间**的事实，不是关于健康度的，所以给它一个**形状**——标记右上角一个缺口，颜色继承标记自己的前景色。往哪个方向动，用**词**写在有地方写的三处：tooltip、点开后的详情条、以及矩阵头部那行（`2 worse · 1 better since 09-11`）。

## 2j. 一个从来没工作过的面板（0.31.0）

### SEPA table stats：10 张表全部读失败，画成红色的 0/10

`coverage/sepa-stats` 把 schema 写死成 `market.`。自 wave relocate 起 `market` 只是 `raw_market` 的**别名**，而 `resolve_market_schema` 只在**守卫**里被调用：

```python
if not table_exists(conn, schema, table):     # ← 解析别名 → True
    ...
cur.execute(f"SELECT COUNT(*) … FROM {schema}.{table}")   # ← 不解析 → UndefinedTable
except Exception:                              # ← 裸 except
    tables.append({"row_count": None, "latest": None})
```

实测 10 张全部抛 `UndefinedTable`。持有 1,373 万行的 `stock_daily`、239 万行的 `option_open_interest`、200 万行的 `option_snapshot`，在这个面板上读成十张空表，头部标签写 **`0/10 today`** 并且是红的。

**「守卫解析、查询不解析」这个形状值得单独记住**——它不会报错，它会让一个坏读法看起来像一次成功的坏结果。同一个 bug 在 `query_distributions` 里也在，而且更糟：那里没有 catch，直接 500（该端点 Console 零消费，所以没人撞到）。

### 为什么是退役而不是修

就算把 schema 解析上，**10 张里还有 3 张仍然坏**：

| 表 | 修好 schema 之后 |
|---|---|
| `option_daily` | `COUNT(*)` 超时——实测 3,731 万行，180s 预算不够 |
| `stock_financials` | `updated_at` 列不存在（wave 8 拆表后它是三张表上的兼容视图） |
| `corporate_action` | 同上 |

也就是说**表清单和列清单都是过时的**，是两处独立的腐烂。而 `db-summary` 一直在用 `safe_count`（它**会**解析别名）回答同一个问题，Coverage Matrix 已经在四个轴上覆盖这些表。留着它等于在 db-summary 旁边并存**第二份手写表清单**和**第二个「今天算新鲜」的规则**——正是 C-G1 禁止的东西，也是新鲜度「被 7 个面板回答」里的一个。

`stock_minute`（0.4s / 315,891）与 `stock_snapshot`（0.3s / 288,982）并入 `db-summary`；`option_daily` 以 `pg_class.reltuples` **估算**并入（0.0s / ~3,731 万），payload 用 `estimated` 数组点名，UI 前面加 `~`——**估算值和精确值不能长得一样**。`stock_financials` 没有并入：COUNT 要 30.3s，而它是三张已在矩阵里单列的表上的视图，并入等于给同一批行第四个名字。

### 静止的表头（同版）

Option chain coverage 与 Stock historical depth 的裁决**被关在细节后面**：前者 21s + 52s、后者每个 watchlist 标的一次请求，都只在展开时才跑，所以折叠状态下表头是空的——恰恰是最想知道结论的时候。

- **Option chain**：新增 `coverage/chain-headline`，后台缓存。汇总**不会**让它变便宜——卷成一行的 greeks 仍要 40.5s，成本在 200 万行快照上的 `DISTINCT ON`，不在返回的行数——所以走 `BackgroundCache`，和四轴页、quality score 同一个模式。
- **顺带修掉一个假比率**：Console 按 `limit=500` 取，环上写 `301/500`；实际是 **379/570**。分子和分母被同一个分页上限截断，**两半都是错的**。headline 不分页。
- **然后我在同一个 headline 上犯了 C-B1（0.31.1 订正）**：新端点的 greeks 口径是照抄 `query_greeks_coverage` 的 `DISTINCT ON (option_ticker)` **跨全历史**——一张八月最后被快照的合约，会拿它八月那一行进分母。实测 2026-09-10：**全表被快照过 244,548 张合约，而当晚链里只有 187,456 张**，掉出去的 57,092 张背走了大部分缺失的 greeks。全历史口径读 **86.1%**，当场自己的口径读 **93.8%**。改成按 session 收敛，两半来自同一场，payload 带上 `session` 日期——这样 14:30 盘中链只有基准的那种「半写完的 session」是**样本小**而不是**读数假**。

### 顺着这条线查下去：那 1.8 万张「有持仓无 greeks」

Owner 的问题是对的，答案分两半，而且只有一半是真的：

| | 张数 | 未平仓合计 | 是什么 |
|---|---|---|---|
| IV 和 greeks 都缺 | 10,927 | 468 万 | vendor 什么都没算出来 |
| **IV 有、greeks 缺** | **7,480** | **1,079 万** | 看着最可疑的一类 |

把范围收到**当晚那一场**，第二类**一张都不存在**：09-10 全场 486,503 行有 IV，486,503 行有全 greeks，两个数字一模一样；09-09 相差 1,096 行。也就是说那 7,480 张**全部是旧行**——它们早就掉出取数窗口（core/edge 只取现价 ±15% 的带），最后一次被看见时 vendor 给了顶层 `implied_volatility` 但没给 `greeks` 对象，之后再没被覆盖。

当场真正缺的是 10,693 张（分布在 503 个标的上，平均每个标的约 21 张），**它们连 IV 都没有**——vendor 对这些合约什么都没算。占当场有持仓合约的 **2.9%**。

结论：**不是采集缺陷，是口径把陈旧行算进了当下。** 修的是口径，不是数据。
- **Stock depth**：不新增端点。`quality-score` 的 `stock_daily_coverage` 已经在同一个窗口、同一份 watchlist 上算过 `gap_count`，且已经缓存、已经在页面上以 4/4 卡片显示。表头读它——不多打一次请求，也**不多一个「什么算缺口」的定义**。

## 2k. 记忆上线第一晚就抓到一个瞬态（0.31.2）

矩阵有记忆的第一个晚上就记到了变化，而且是一对：

```
20:24 → 22:03   stock_daily 厚度   ok → partial     ↓
22:03 → 22:10   stock_daily 厚度   partial → ok     ↑
```

22:00 正是 `eod-pipeline` / `stock-eod` 点火的时刻。当天这一场正在写入，表里只有一小部分行，稀薄检测把它判成了洞。**读数是对的，判定是没用的**：它每晚都会重演一次，每次往记忆里写两行，而那十分钟里任何人点开 Agent 简报都会看到 `WORSE · stock_daily continuity: ok → partial`。

修法的原则已经写在 doctor 里了——它算缺失日时只取 `[first, last]` 区间，注释就是「**the newest session may simply not be due yet**」。缺的是把同一条规则从**缺失**延伸到**稀薄**：`measure()` 现在接受 `session`（即 `session.resolve_session` 那一个定义，C-F1），晚于它的日子**保留在 `days_present` 里但不参与稀薄判定**，并以 `days_not_due` 点名。

「在，但还没到该判断的时候」和「干净」是两回事，也和「缺失」是两回事——面板因此写 `81 clean · 1 still writing` 而不是 `82 sessions clean`，否则那个数会在 EOD 窗口里自己跳动而没人知道为什么。

### 这次记录暴露的一个设计代价

记录只存**判定**，不存读数。所以事后能看到厚度变过，看不到 22:03 那一刻具体是「今天只有 18 行」还是别的——22:00 这个时刻是很强的旁证，但严格说**没有证据**。这是当初的取舍（76 个判定 vs 整份 payload），方向仍然是对的：判定才是算不回来的那个东西，读数原则上可以重算。但「原则上」在这里失效了——那一刻的读数也随时间消失了。值得记下来，暂不改：存整份 payload 的成本远大于它的用处，而真正需要事后归因的场合，`queue_sample` 和 job 历史还在。

## 2l. 养库缺的是斜率，不是状态（0.31.3）

四个轴把「**是什么状态**」答完了。但养库关心的是**斜率**——在长吗、多快、还差多久。这一半此前基本没有呈现，最明显的一处是累积进度：

空心标记里那段填充报的是 `sessions_held / accrues_to`。**一个「爬到 60%」的边界，读者无法分辨它是昨晚还在涨、还是三周前就停了**——而当目标是养库时，这恰恰是唯一值得区分的两种状态：一种需要耐心，另一种需要去看一眼。

### 速率从现成的 skip scan 里榨出来

`_ACCRUAL_SQL` 本来就是一次递归跳跃扫描，枚举所有不同的日期。加一个 `count(*) FILTER (WHERE t::date > CURRENT_DATE - 14)` 就得到「最近 14 天攒了几场」，**不多一次查询**。

关键是分母：`gained / 窗口内实际的交易日数`，不是 `gained / 14`。**市场关门的一周不是停滞的一周**，用日历天当分母的速率会让每个数据集在圣诞节集体上报警。日历 `_expected_days` 在 `_compute` 里已经读过一次，传下去即可。

三条边界情况写成了显式规则而不是让它们悄悄退化：

- **窗口内没有交易日** → `rate: null`、`stalled: null`。没有速率可报，硬报一个就是把假期变成假警报。
- **已经到顶**（`held >= accrues_to`）→ `stalled: false`。它不长是因为没地方可长了；它还在不在被写入是**新鲜度**的问题，不是这一轴的。
- **没有日历** → 同样 `null`，不是「一切正常」。

`sessions_remaining` 报的是**交易日数**不是日期：投影一个日期需要前向日历，用五天工作周凑一个出来是把猜测打扮成度量。

### 画在哪里

**不加第五种颜色。** 矩阵的两把尺子（颜色=等级、填充=有没有判定）不动；「它停了」和「它变过」一样是关于**时间**的事实，都放在网格上方那一行用词说清楚：`2 of 3 accruals stalled`，tooltip 点名是哪几个。标记自己的 tooltip 从「accruing, 60% of the way」改成「stalled at 60% of the way」——同一个填充，不同的说法。

Agent 简报新增一节，并且明确区分它和缺口：**停滞的累积不能回填**（vendor 不事后卖这些 session），要看的是采集器还在不在跑，它错过的每一场都永久没了。「not clean」那一节从来不提边界，所以在这之前，一个 Agent 无从得知一个本该攒到 90 场的堆在两周里一场没涨。

## 2m. 一次超时背后的两个真问题（0.31.4）

迁移 Job 的 `adjusted_root_repair` 今天两次撞上 120s 语句超时，都在库忙的时候，靠 Job 重试兜底。查下去发现超时只是症状。

### 一、还有 296 万行挂在旧根下

```
option_daily WHERE underlying = ANY(36 个旧根)   →  2,964,147 行
option_minute                                    →  0 行
```

**这订正了本文档 §2f 记下的一句话**：「repaired 89,790 rows，幂等（第二次运行没写任何东西）」。第二次没写东西**不是因为没得修，是因为它超时并整体回滚了**——所有重写都在迁移的同一个事务里，第 40 条语句超时就把前 39 条一起撤销。而回滚之后的下一次运行报「没写任何东西」，读起来和「没有东西要写」一模一样。

**一个失败的形状伪装成了一个成功的形状**，而且它自己制造了掩盖自己的证据。

### 二、造出这些行的入口一直没修

`option_backfill.py` 给 `option_daily` 排队时，payload 只有 `{option_ticker, from, to}`——**没有 `underlying`**。而 canonical underlying（`storage`）就在同一个函数里，函数末尾还返回它。所以每个回填 job 的 handler 都退回去解析 ticker，一张调整后合约就落在 `BDX1` 这个自造的符号下。

`option-bars` 从 0.21.x 起就带了这个字段；这个计划器被漏掉，正因为 `storage` 早就在作用域里、早就是对的，只是从没被放进 payload。

按 `bar_date` 分布可以确认这是**历史积压不是持续泄漏**：2–7 月每月约 15 万行，8 月 3.6 万，**9 月只有 425 行**——P4 那两年回填窗口造的。但只要入口不修，**下一次回填会重新造一遍**。

### 修法

三处，缺一不可：

1. **入口**：回填计划器把 `storage` 放进 payload。两行。不修这个，其余都是跑步机。
2. **逐根提交**：修复改为拿 connection 而不是 cursor，每个 `(表, 根)` 提交一次。296 万行不可能在任何合理预算内一次改完，所以它必须**留下已经做完的部分**——从「永远做不完且看起来做完了」变成「每次部署啃掉一块」。一条语句超时只损失那一个家族。
3. **不许拖垮部署**：schema 先单独提交，修复在其后且整体包在 `try` 里。部署需要的是 schema；这是搭车的数据修复，做不完就顺延。实测 2026-09-10，正是它让一次**成功的部署**最后一行打印 `DDL failed`。

预算 90 秒——它骑在部署上，一个要等十分钟的部署没人会跑。

## 3. 已知差距与最小改动

### 3.1 重复与冲突（最大的一类）

| 差距 | 证据 | 最小改动 |
|---|---|---|
| 新鲜度被至少 **7 个面板**回答 | `WorkersFreshnessPanel` / `DataVitalsStrip` / `SepaStatsSection` / `QualityScoreSection` / `QueueDashboardPanel` / `DoctorPanel` / `HusbandryStrip` | 收敛到契约表的截止时间，面板只渲染 |
| ~~陈旧阈值 4 套~~ | Plugin 侧已收敛（C-F3）；platform-api 与 Console `dataVitalsModel.ts` 各留一份 | 那两份读契约表 |
| ~~session 定义 4 套~~ | 已收敛到 `session.py`（C-F1） | — |
| ~~分母 4 套~~ | 已收敛到 `contracts.py` + `scopes.py`（C-B1） | — |
| ~~永远 100% 的假分母~~ | `DataInventoryStrip` 三处 + `analyticsDemandModel` 四处已修（C-G1） | — |
| `/coverage/quality-score` 被三处 fetch | `MarketDataOverviewTab.tsx:68-73`、`QualityScoreSection.tsx:46-51`（共享缓存）、`ReadinessPanel.tsx:70-75`（**独立 queryKey，真重复请求**） | 统一 queryKey |
| `/coverage/contracts` 被两处不同 limit 打两次 | `DataVitalsStrip.tsx:105-110`（默认 100）与 `OptionCoverageSection.tsx:159`（500）——Vitals 卡上的 contracts 只是前 100 名之和，与 Coverage 页对不上 | 同一 limit 或同一 query |
| 纯 alias 端点 | `coverage.py:701-707` `stock-day-quality-detail` = `bar-quality-detail` | 删一个 |

### 3.1b 打不开的面 → 已修（0.19.7）

回填负载下，三个端点在 pod 里能算完，但都超过 platform-api 的 60 秒网关：

| 端点 | pod 内耗时 | 之前 | 现在 |
|---|---|---|---|
| `/market/coverage/dimensions` | 80 秒 | 已是后台算 + 缓存 | 同左，改用共用实现 |
| `/market/coverage/inventory` | 141 秒 | 502，Analytics demand 卡在 "Loading inventory…"、六个产品全判 blocked | 立刻回上一次的答案并报年龄 |
| `/market/readiness/summary` | 81 秒 | 502，Readiness 页拿不到数 | 同上；后台无网关，两条子查询拿到 600 秒预算，不再靠降级返回 |

**不是把查询变快，而是不让读的人等。** 存量口径本来就快不了：inventory 最宽的一条是对 1,363 万行 `stock_daily` 做一次全表 distinct 计数，
2026-09-09 实测 151 秒，而**去掉 `UPPER(TRIM())` 反而更慢**（401 秒，两种写法都要读全表），所以谓词不是问题。
共用实现在 `api/slow_cache.py`：立刻回上一次的答案、后台重算、明说年龄与"是否有更新的在路上"，同一个 key 同时只跑一次。

配套改了 Console 一处判断：**"还在数"不等于"数完了是零"**。之前 inventory 取不到数时六个产品全判 blocked，
现在 `computing` 期间判 unknown 并保持加载态，拿到数后照常评级，卡片上带年龄标签。

### 3.1c 队列的吞吐（2026-09-09 实测与修复）

| 项 | 修之前 | 修之后 |
|---|---|---|
| worker 有效并发 | 3.8 / 40 槽位 | 20–38 |
| 消化速率 | 666/分 | 2,200–2,700/分 |
| 240 万回填 ETA | 60 小时 | 约 15 小时 |
| trim | 自 2026-09-08 02:15 起从未跑完 | 分批，单轮 66 万行 |

根因三条，都是先量后改：

1. **满负荷被当成空闲**。worker 循环在 `len(in_flight) >= max_concurrency` 时跳出取任务，此时 `claimed_any` 仍是 False，于是走到空闲分支 sleep 满 5 秒的轮询间隔——恰恰是最该继续取任务的时刻。每个 pod 跑 5 个、睡 5 秒、循环。**claim 本身从不是瓶颈**：在真实竞争下连测 240 次，次次取到，耗时 1 毫秒。
2. **trim 的行数上限排序无索引可用**。`ORDER BY finished_at DESC NULLS LAST, id DESC` 与 `job_ingest_finished_at`（`finished_at DESC`，而 DESC 本就是 NULLS FIRST）不匹配，顺序扫 127 万行并溢出 32MB 外部排序，14 秒还没删到一行，然后要在 60 秒预算里一次删掉 123 万行。现在游标查询 60 毫秒，两遍都按 20,000 行分批提交。
3. **trim 不带自己的语句预算**。批次是按 API 连接的 60 秒设计的，而 Dagster 触发的 CLI 用的是 `bifrost` 角色默认的 2 秒。现在每批和游标查询都在自己的事务里 `SET LOCAL`。

**一个需要留意的副作用**：trim 的上限是按**行数**（40,000 条已完成）而不是按时间。吞吐涨了 4 倍之后，这 40,000 条只覆盖约 15 分钟，所以 doctor 的"24 小时内失败任务数"实际只能看到十几分钟。失败计数在 `queue_sample.failed_delta` 里按 5 分钟永久留存，但 doctor 那条查询会低报。这是既有策略在新吞吐下暴露出来的，不是本次引入的。

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
| 2026-09-10.8 | 2026-09-10 | §2h：五类度量偏差全部修完（财报节奏、深度 population、绝对起点、不该问的广度、轮转口径），`5/19 clean` → `8/19`。含一个被数据否定并撤回的假设（`common-stock` 档位）、`ratios` 缺口的结论（两个重叠集合，非缺口）、以及档位定义的声明化。|
| 2026-09-10.7 | 2026-09-10 | 广度改为按 session 界定（曾把盘中的 26 个除以 575）；`backfill_slot` 的三义 `None` 改为显式 `Refill`。新增 §2g：矩阵照出 14 条 not clean 里只有约 4 条是真缺口，其余是分母/目标/节奏定义错。|
| 2026-09-10.6 | 2026-09-10 | `option_snapshot` / `option_open_interest` 深度改为 `forward_only` 计划边界（Owner 决定）：链快照只能向前累积，`sessions/90` 把爬坡报成了缺陷。厚度轴仍量它们，爬坡进度不丢。|
| 2026-09-10.5 | 2026-09-10 | Coverage 分层：宏观（inventory + 4/4 + 档位×粒度矩阵）全部毫秒级，细节展开才取；11 个端点降到 5 个。契约新增 `grain`。顺带修掉「还在算被渲染成 PASS」和「没有时钟被渲染成 unknown」两个误导判定。|
| 2026-09-10.4 | 2026-09-10 | 期权目录轮转：标称 12 天、实测 ~48 天（轮转偏移按日期哈希，一天四次运行取同一批）。改为按 `updated_at` 最旧优先 + `batch_size` 144，一天一圈。|
| 2026-09-10.3 | 2026-09-10 | 厚度检查的查询形状三次修正（§2f 末）：聚合 → 逐日探测 → 一次往返 → LATERAL。第三步曾让 `option_daily` 静默超时被跳过，而它正是这条检查的主体。 |
| 2026-09-10.2 | 2026-09-10 | Doctor 接上厚度轴（§2f）：60 天窗口内找出可补的缺失 session 并给出精确到日的处方，`market_self_heal` 因此获得修补旧洞的能力；能不能补由契约的 `backfill_slot` 声明，19 个数据集里 5 个可补。调整后合约的 underlying 归一到目录表，历史行由幂等迁移订正。 |
| 2026-09-10.1 | 2026-09-10 | 队列跑空后的四轴普查（§2e）。一个解析器缺陷让所有调整后期权合约（`WDC1`/`XOM2`/`SPGI1`…）拿不到日线；`option_daily` 的两年历史本会从回填结束当天起停止前进，Owner 定为扩 `option-bars` 到全宇宙（69,950 job/交易日）；四处「量错了」中的三处已修。 |
| 2026-09-09.11 | 2026-09-09 | 厚度轴照出的最大一处已修：`short_volume` 原本只有 2 天真数据，契约把它误标为不可回填（抄自 `ratios`）。改为 `rolling_days` 730 天并补齐 496 个交易日，501/501。 |
| 2026-09-09.10 | 2026-09-09 | 第四轴：厚度（C-C1–C-C4）。蓝图升到 v1.1。首次读数见 §2c——`stock_daily` 的 7 个空洞已补，`short_volume` 三个月的缺口首次可见。 |
| 2026-09-09.9 | 2026-09-09 | 队列吞吐 666→2,400/分（满负荷被当成空闲）；trim 分批后恢复；新增 `ops_jobs.queue_sample` 与 Console 的 Queue history 曲线，C-D4 转 ✅（✅ 7 / ⚠️ 6 / ❌ 1）。详见 §3.1c。 |
| 2026-09-09.8 | 2026-09-09 | Console 实测复核：Stock daily 的表把"有史以来的标的"除以"今天活跃的 ticker"，改为同源读数 5,182/5,317；无条的表现在会说清缺的是口径还是数字。Overview 从"blocked 6"变为"ready 6"。 |
| 2026-09-09.7 | 2026-09-09 | §3.1b 的两个端点改成后台算 + 缓存（共用 `api/slow_cache.py`，dimensions 一并迁过去）。Console 区分"还在数"与"数完是零"。 |
| 2026-09-09.6 | 2026-09-09 | 记下 §3.1b：`coverage/inventory`（141 秒）与 `readiness/summary`（81 秒）都超过 60 秒网关，Overview 与 Readiness 两页因此取不到数。 |
| 2026-09-09.5 | 2026-09-09 | 收敛后的两次实测各抓到一个错：benchmark 档的分母把 watchlist 丢了（11 而非 26，`stock_minute` 因此报 3/11 而非 18/26）；`fundamentals_market` 拿纽约午夜当截止时间，夏令时每晚有半小时报假 critical。两条都是"新仪器照出自己的毛病"，不是新引入的缺陷。 |
| 2026-09-09.4 | 2026-09-09 | 分母收敛完毕（C-B1 ✅）。Doctor 的巡检面从 28 个名字扩到 575，按采集方式分档判定；顺带发现并修掉第五处假分母（`analyticsDemandModel` 的四个），以及 `k8s/base` 钉在一个从未构建过的 tag 上。契约状态 ✅ 6 / ⚠️ 7 / ❌ 1。 |
| 2026-09-09.3 | 2026-09-09 | 口径收敛：`session.py` 一套 session 定义，阈值全部派生自契约表；Console 的假分母改为除以真分母、无分母不画条；顺带修好 quality gate 在回填负载下的 500。契约状态 ✅ 5 / ⚠️ 8 / ❌ 1（上一轮 ✅ 3 / ⚠️ 5 / ❌ 6）。 |
| 2026-09-09.2 | 2026-09-09 | 契约表代码化（`contracts.py`，19 个数据集）+ `/market/coverage/dimensions` + Console 三维表。三维首次可读（§2b）。契约状态 ✅ 3 / ⚠️ 5 / ❌ 6。 |
| 2026-09-09.1 | 2026-09-09 | 基线。三维首次实测；14 条契约中 ✅ 0 / ⚠️ 4 / ❌ 10。深度维度确认为最大空白（575 个标的里 47 个有期权历史，零面板显示）。 |
