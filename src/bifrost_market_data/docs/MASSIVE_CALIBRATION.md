---
version: 2026-09-10.1
updated: 2026-09-10
status: 队列跑空后的四轴普查 · 一个解析器缺陷、一处会烂掉的历史、四处量错了
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

## 2d. 队列跑空后的四轴普查（2026-09-10，Plugin 0.21.0 / 0.21.1）

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

## 2c. 本轮收敛（2026-09-09，Plugin 0.19.0 / 0.19.1）

**四套 session 定义 → 一套。** `session.py` 持有唯一定义与截止时间算术，从**收盘**起算而不是从午夜——一个 22:00 落库的数据集不该在次日早上显示成快一天旧。迟到需要同时满足两件事：**截止时间已过**，且**收盘后没有任何一次运行**。于是"周五晚的数据在周一早上"自然是待定而非陈旧，旧规则为此专门开的 72 小时周末例外**不再需要存在**。

**四套阈值 → 契约表。** doctor 的 `STALENESS` 改为从 `contracts.staleness_by_slot()` 派生，一条测试断言两者逐条一致。**巡检范围仍由 doctor 显式声明**（`POLICED_SLOTS`）——派生表覆盖 16 个 slot，直接放开会把 verdict 从 healthy 变成 degraded，那是"什么该告警"的决定，不该夹带在收敛里。`corporate` 保持 168 小时：分红拆股本就稀疏，48 小时会去告警日历而不是告警数据源。

**顺带修好一个既有脆弱点。** quality gate 的 `count(DISTINCT symbol) FROM stock_daily` 在回填负载下**超过 10 分钟**，整个 gate 返回 500。`stock_daily` 按年分区，跨分区 DISTINCT 无法提前收敛，加 `LIMIT` 也没用。改为问 doctor 一直在问的那个问题——**当期 session 的宽度**（单分区等值扫描，10 秒），读数 12,518 个标的对 4,000 的下限，verdict 不变，但在回填期间够得着了。

线上验证（0.19.1）：doctor 19.8 秒五条 staleness 全 ok；quality-score 从 500 变为 23 秒，freshness 按维度给出各自截止时间（2h / 2h / 2h / 48h），平铺阈值为 `None`。

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
| 2026-09-10.1 | 2026-09-10 | 队列跑空后的四轴普查（§2d）。一个解析器缺陷让所有调整后期权合约（`WDC1`/`XOM2`/`SPGI1`…）拿不到日线；`option_daily` 的两年历史本会从回填结束当天起停止前进，Owner 定为扩 `option-bars` 到全宇宙（69,950 job/交易日）；四处「量错了」中的三处已修。 |
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
