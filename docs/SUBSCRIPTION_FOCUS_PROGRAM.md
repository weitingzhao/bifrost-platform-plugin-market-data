# Subscription Focus Program — 把三个 Massive 订阅挖干净

**Program**: `market-data-subscription-focus` · **Owner 决策日**: 2026-09-06 · **Spine**: D10 BLOCKED（observe-only，本程序不触碰交易执行）

Owner 策略：**升级订阅之前，先把 Options Starter、Stocks Starter、Financials & Ratios 三个订阅的数据能力全部有效利用，支撑 Research 产生业务价值。** 未授权的数据（trades / quotes / last-trade、指数 I:SPX / I:VIX）一律停拉，等升级后再说。

可分享的评估页面：<https://claude.ai/code/artifact/727e00e9-5903-48cd-9c50-118171f1823a>

---

## 1. 实测授权矩阵（2026-09-06，在 `market-data-api` Pod 内逐端点探测）

| 能力 | 端点 | 结果 | 窗口 / 限制 |
|---|---|---|---|
| 股票日线 / 分钟线 | `/v2/aggs/ticker/{sym}/range/…` | 200 | **滚动 5 年**（2021-09-07 可取，2021-08-30 → 403） |
| 全市场分组日线 | `/v2/aggs/grouped/…/{date}` | 200 | 一次 10,940 只；4 年前日期可取 |
| 全市场快照 / 涨跌榜 | `/v2/snapshot/locale/us/markets/stocks/…` | 200 | 一次 13,159 只；15 分钟延迟 |
| 技术指标 / 新闻 / 事件 / IPO / 关联公司 | `/v1/indicators` · `/v2/reference/news` · `/vX/reference/tickers/{sym}/events` · `/vX/reference/ipos` | 200 | — |
| 股票逐笔 / 报价 / 最新成交 | `/v3/trades` · `/v3/quotes` · `/v2/last/trade` | **403** | 需 Developer / Advanced |
| 期权链快照 / 单合约快照 | `/v3/snapshot/options/{und}[/{contract}]` | 200 | 每页 250；含 greeks / IV / OI |
| 通用快照 | `/v3/snapshot?ticker.any_of=…` | 200 | 一次 250 个 ticker |
| 合约目录（含已到期） | `/v3/reference/options/contracts?expired=true` | 200 | 可枚举到 2022 年；每页 1,000 |
| 期权日线 / 分钟线 | `/v2/aggs/ticker/O:…/range/…` | 200 | **滚动 2 年**（2024-09 可取，2024-08-16 到期合约 → 403） |
| SPX 指数期权链 | `/v3/snapshot/options/SPX` | 200 | 属 Options 订阅 |
| 指数行情 | `/v2/aggs/ticker/I:SPX` · `/v3/snapshot/indices` | **403** | 需 Indices 订阅 |
| 期权逐笔 / 报价 | `/v3/trades/O:…` · `/v3/quotes/O:…` | **403** | 需 Options Developer / Advanced |
| 三大报表 | `/vX/reference/financials` · `/stocks/financials/v1/*` | 200 | 2009 年起，含 `filing_date` |
| 财务比率 | `/stocks/financials/v1/ratios` | 200 | 按 `date` 一次取全市场，1,000 / 页 |
| 空头持仓 / 空头成交量 | `/stocks/v1/short-interest` · `/stocks/v1/short-volume` | 200 | 可按日期取全市场 |
| Float / SEC filings | `/stocks/v1/float` · `/stocks/filings/*` | **404** | 路径不存在 |
| 国债收益率 / 通胀 | `/fed/v1/treasury-yields` · `/fed/v1/inflation` | 200 | 免费，1962 年起 |

**限流实测**：连续 15 次请求 0.9 秒全部 200，无 429。付费 Starter 是无限调用。

---

## 2. 阶段与进度

| Phase | 内容 | 状态 | 日期 |
|---|---|---|---|
| P1 止血 | 修 UnboundLocalError；限流改为付费档；停未授权拉取；EOD 去重（session-once、触发日 holiday skip、OI 随快照写、expiration 随合约写）；维护 slot 退出 gate；删 `trades-quotes` / `filings` / `float` 死路由；依从性证据改为 freshness 兜底 | ✅ 0.10.4 已发布（Console 验收观察 5 个交易日） | 2026-09-06 |
| P2 收敛 | slot 按授权矩阵重排；新增 ratios / short 全市场日更；宇宙卫生 + 按 symbol 的 void；watchlist 缓存；重活出 API；Dagster RetryPolicy + 告警；未授权能力的占位说明（API / Console / Trade UI） | ✅ Plugin 0.11.1 + Dagster 0.67.0 已部署（验收观察中） | 2026-09-06 |
| P2.5 Doctor | 手动自检 + 一键修复 + 可执行的 Agent 报告 + 每晚自愈：`GET /market/doctor` / `POST /market/doctor/heal`；Console Doctor 面板；MCP `market_data_doctor` / `market_data_heal`；Dagster `market_self_heal`（00:45 UTC 周二–六） | ✅ Plugin 0.12.0 + Dagster 0.68.1 | 2026-09-06 |
| P3 模型 | `snapshot_ts` 重定义为观测时间（保留列名与主键形状）；OI 从快照派生、退役 gap-heal；job 幂等键按 session；健康判定按会话覆盖率 | ✅ Plugin 0.13.1 + Research 0.92.0（Owner 2026-09-08 批准方案 A） | 2026-09-08 |
| P4 挖掘 | 5 年股票 / 2 年期权回填；日内链快照；国债收益率；Research staging 契约修正 | 🔄 Plugin 0.15.0 + Research 0.93.0 已发布，回填运行中（Owner 2026-09-08 批准 A/A） | 2026-09-08 |

### P1 改动摘要（0.10.3）

- `scheduler/daily.py`：删掉 oi-gap-heal 分支的局部 `import update_freshness`（同函数 trim 分支因此抛 UnboundLocalError，是 9-06 Console 红灯的直接原因）；新增 `fire_date` 参数，holiday skip 以**触发日**判断（周六触发不再回滚到周五重跑）；`SESSION_ONCE_SLOTS`（stock-eod / eod-pipeline）12 小时内已有 job 则跳过，`force` 可绕过；eod-pipeline 只入队 `option_snapshot`（带 `trade_date`），不再入队 `option_open_interest` 与 I:SPX；option-refresh 不再入队 `option_expiration`；`option-trades` 标记 unentitled，调用返回 skipped 而非 400。
- `ingest/option_snapshot.py`：同一次下载同时写 `option_snapshot`、`option_contract`、`option_open_interest`（`trade_date` 取 payload 或 NY 会话锚点），结果携带 `freshness_extra`；`worker/loop.py` 据此同时刷新 `option_open_interest` freshness。
- `polygon/rate_limit.py`：`basic` = 5/min（免费档）；`starter` = 8 req/s、突发 16（付费档软上限）；`bucket_from_config` 支持 `polygon.rate_per_sec` / `burst` 覆盖。
- `api/ingest_dashboard.py`：trim / oi-gap-heal 标记 `maintenance`，不再决定 schedule / husbandry verdict（单独列 `maintenance_missed`）；`option-trades` 标记 retired。
- 删除 `api/trades_quotes.py`、`api/filings.py`、`/fundamentals/float` 路由及对应 client / endpoint 函数；`k8s/base/cronjob-option-trades.yaml` 删除；schedule 配置移除 option-trades。
- ConfigMap：`polygon.rate_per_sec: 8`、`burst: 16`；worker `concurrency: 8`、`job_timeout_sec: 1200`、`stale_running_sec: 1800`。

### P1 发布记录

- 0.10.3（2026-09-06 17:40 UTC）：上述全部改动；集群实测 trim 后 `job_trim` freshness 更新、周日 eod-pipeline 返回 `non_trading_day`、option-trades 返回 `unentitled`。
- 0.10.4（同日）：手工跑 trim 后发现 `keep_max: 5000` 会删掉上一 session 的 job 行，6 个只靠 job 行作证据的 slot 立刻变 missed。修法：每个 slot 都指定 freshness 维度（freshness 行不被 trim），`keep_max` 提到 40,000。
- 注意：Kaniko 从 Gitea 镜像克隆，推 GitHub 后必须先 `make -C bifrost-trade-infra k3s-sync-gitea-mirrors`（macOS 没有 `timeout`，别用它包 make），构建后用临时 Pod 打印 `__version__` 确认再 apply。

### P2 改动摘要（0.11.0 · research 0.67.0）

- **占位而非删除（Owner 2026-09-06）**：`subscription.py` 是唯一的订阅事实源；`GET /market/capabilities` 给出 entitled / planned / unavailable 三类能力与升级所需套餐；queue-dashboard 把 `option-trades` 作为 `retired · planned_on_upgrade` 行保留在计划表里；Console Ingest 页新增「Subscription coverage」面板；Trade UI Discovery 流动性面板保留「Trades & Quotes — Not in the current plan」卡片。
- **按授权矩阵重排 slot**：`corporate` 改为全市场按除权日窗口拉分红 / 拆股（两个 job 替代每标的两个）；新增 `fundamentals-market`（04:30 UTC 周二至周六）按日期拉全市场 ratios、short volume 与最近 settlement 的 short interest；`option-bars` / `minute-bars` 改为按最新收盘价选 ATM ±N 档 × 最近 N 个到期；合约目录分页上限 20 → 60（SPX 120）。
- **宇宙卫生**：`ticker_sync` 全量列表未截断时把 vendor 不再列出的名字置为 `active=false`；新表 `ops_jobs.symbol_source_void` 记录 vendor 无数据的 symbol，`financials` 空结果写入、非空清除，rotate 跳过 30 天内确认的 void。
- **watchlist 缓存**：新表 `ops_jobs.watchlist_cache`；platform-api 可达时刷新，不可达时回退到缓存而非空列表。
- **重活出 API**：`oi-gap-heal` 改为 `oi_gap_heal` worker job（每个 job 5 个标的，逐标的 SELECT）；enqueue-slot 改为单语句批量插入（`insert_jobs_bulk`）。
- **Dagster（research 0.67.0）**：所有 market slot asset 带 `RetryPolicy(3, 60s, exponential)`；`bifrost_run_failure_alert` sensor 把失败推到 Alertmanager 的 Bifrost 路由；`market_corporate_trades` → `market_corporate`；新增 `market_fundamentals_market_schedule`。

### P2 集群实测（2026-09-06，0.11.0）

- `fundamentals-market` 手工触发：ratios 6 页 5,016 行、short volume 16 页 15,160 行落库（此前 22 行 / 240 行）；short interest 20 天窗口为 0 行 → 0.11.1 改为 45 天（FINRA 结算后约 10 天才发布）。
- `oi-gap-heal` 6 个 worker job 全部完成（每 job 5 个标的，最大 28 万候选行），API Pod 不再参与。
- `reference` 全量列表把 61 个 vendor 已不再列出的名字置为 inactive。
- `watchlist_cache` 已缓存 18 个 symbol。
- 0.11.1：仪表盘 cron 解析支持 `2-6` 这类范围（`fundamentals-market` 曾显示 unsupported_cron）。

### P2 部署记录

- Plugin 0.11.0 → 0.11.1（2026-09-06 18:30 UTC 前后）：API + 7 个 worker 全部就位；`ops_jobs.symbol_source_void` / `watchlist_cache` 已建（postgres 建表，bifrost / data_writer / analytics_writer 授权）。
- Dagster 0.67.0-dagster（18:36 UTC，`kubectl apply -f k8s/orchestration/dagster.yaml`；该目录被 Argo 排除，不走自动同步）：daemon 自动清掉了 `market_corporate_trades_schedule` 的旧状态，`bifrost_run_failure_alert` sensor 已在轮询。
- 未纳入 P2、留给 P3/P4：Research 侧 dbt staging 的字段契约（`stg_short_*` 驼峰、`stg_ratios` 空占位）在 P4 与回填一起改；Console 检查清单里提到已删路由的文案。

### P1 验收

- Console market lane 连续 5 个交易日无误报
- 每日 vendor 请求 ≤ 4,500（此前约 7,400）
- EOD 期权窗口 ≤ 15 分钟（此前约 2 小时）
- `job_ingest` 每日失败行 = 0（不含 vendor 空结果）

### P1 未纳入（留给 P2）

- Research `market_corporate_trades` 资产仍会调用 `option-trades`，现在得到 `skipped: unentitled`，无害；随 P2 的 dbt 契约修正一起改 Dagster。
- Trade 前端 Option Discovery 的 last-trade / quotes 调用另行提交（前端仓库）。

### P2.5 Doctor（0.12.0 / Dagster 0.68.1）

Owner 的判断：全自动自维护但每天照样失败且无法自愈，等定时任务是设计缺陷。P2.5 把「看见问题 → 知道缺什么 → 立刻补上 → 确认补上了」做成一条链，四个入口共用同一份处方。

- **`GET /market/doctor`**（`doctor.py`）：以「此刻表里应该有的 session」为基准（交易日 19:30 纽约时间后算当天，否则上一个完成的 session），逐项对比应有 vs 实有：期权链快照 / OI 按 optionable 标的覆盖、全市场 `stock_daily`（≥ 4,000 行）、watchlist 日线、`stock_snapshot`、ratios + short volume；calendar / reference / option-refresh / corporate / fundamentals-rotate 的 freshness 年龄；24h 内失败的 job（按 kind 聚合，含样例错误；含 "not entitled" 的不给处方）；卡住的 running；worker `/health`；vendor 一次廉价探测（`/v1/marketstatus/now`，key 走 Authorization 头）。每个 finding 有 `severity` / `expected` / `actual` / `fix` / `auto_fixable`，`prescriptions` 按 fix 去重（快照与 OI 缺失合成一条 `eod-pipeline` 处方，`date` 钉死到 session、`force` 绕过 session-once）。查询都限定在 universe 标的与单日范围，`statement_timeout=120s`。
- **`POST /market/doctor/heal`**（写 token）：`{dry_run, finding_ids}`；执行 `enqueue-slot`（带 session 日期 + force）与 `retry-jobs`（原 kind / payload 重新入队，dedup 生效）；`rollout-restart` / `check-vendor-key` 只报告不执行（插件无权）。
- **Console** `DoctorPanel`（Ingest tab 顶部）：Check now · 每行 Fix · Fix all（ConfirmDialog）· 修复后 60s 自动复查 · 「Copy doctor report for Agent」——粘贴给任何接了 `bifrost-platform` MCP 的 Agent 就能动手（附 MCP 调用与 curl）。
- **平台 MCP**：`market_data_doctor`（viewer, GET）/ `market_data_heal`（operator, POST）进 catalog、stdio server、remediation runner；Massive Feed Recover runner 的工作流改为 doctor → heal → 排空 → 再 doctor。
- **Dagster** `market_self_heal`（`45 0 * * 2-6` UTC，即交易日 20:45 EDT / 19:45 EST）：doctor → 有处方就 heal → 轮询 `queue-summary` 至排空（上限 `MARKET_SELF_HEAL_WAIT_SEC`，默认 900s）→ 再 doctor；仍 critical 才 fail（触发 Alertmanager）。无 RetryPolicy（重跑只会重复入队）。已加入 husbandry 白名单。

不在 P2.5：快照类缺口只有当 session 是「今天」才自动修（历史 session 的快照无法回填，是 P3 主键改造的动机）。

### P3 模型（0.13.0 / Research 0.92.0，Owner 批准方案 A）

**根因**：`snapshot_ts` 填的是合约的**最后成交时间**，不是观测时间。没成交的合约被写回它上次成交那天，于是一次 EOD 抓取的 9,602 行只有 8,593 行落在当天会话，其余散落到最早 08-24；更糟的是后来的抓取会原地覆盖旧日期的行——`snapshot_ts` 落在 08-11～08-18 的行 `fetched_at` 最新是 09-05，也就是 8 月中旬那几天的 IV / greeks / day_close / OI 装的是 9 月的值，Research 回测读到的是错的。每标的每会话覆盖率因此只有 33%～72%（SPY 存 4,815，vendor 当场返回 11,966），P3 验收的 95% 用旧模型永远达不到。

**方案 A（Owner 选定）**：列名和主键形状都不动，把 `snapshot_ts` 的语义改成「观测时间」——EOD 用该 session 的 16:00 NY 锚点，日内用真实时刻；旧值移到新列 `last_trade_ts`。因此 Research 6 个引擎、Trade API、插件 API 和视图一行都不用改，它们现有的 `date(snapshot_ts AT TIME ZONE 'NY')` 查询自动从错变对。

**迁移**（`schema/wave9_migrations.py`，`init_schema.py --wave9-sql | psql -U postgres` 执行，raw_market 属 postgres 所有）：按 `fetched_at` 折算回真正被观测的 session，周末补跑折回它在治的那个交易日，同 ticker 同锚点保留 `max(fetched_at)`。先以 ROLLBACK 全量演练通过再正式执行，8.8 秒完成；944,024 行 → 823,411 行（12 万行被覆盖的重复历史折回本会话），错位残留 0，最后成交时间 823,411/823,411 全部保留。09-04 从散落的 56,590 行变成完整的 116,654 行 / 26 个标的 / 1 个观测时间戳（原来 35,123 个）。

**新护栏**：vendor 的链快照永远是「此刻」的链，所以只有在下一次开盘之前补跑才诚实。`trading_calendar.chain_session()` 给出链当前反映的会话，`handle_option_snapshot` 拒绝任何不等于它的 `trade_date`（返回 `skipped: stale_session`），不再把今天的 greeks 写到过去的会话键上。周六补周五 → 允许；周一开盘后补周五 → 拒绝，doctor 也据此把该缺口标成「已丢失，不是待办」。

**健康判定**：从「有没有行」改成会话覆盖率——每个标的的 `option_snapshot` / `option_open_interest` 对照 `option_contract` 里的活跃合约数，低于 **90%** 即告警（0.13.1 实测校准：vendor 的快照端点本来就比合约目录少返回约 5%，SPY 实测 11,966 / 12,576，所以 95% 按构造达不到；正常会话实测 94%–100%，旧模型坏掉的会话是 33%–72%）。分母只算该会话之前挂牌的合约（`first_seen_at`），否则会话之后新挂的合约会把历史覆盖率越算越低；`stock_daily` / `stock_snapshot` 门槛从 4,000 提到 12,000（实测约 12.5k / 13.2k）。新增 `doctor.eod_critical`：只看会话数据本身（链覆盖、OI、股票日线），Research 的 `husbandry_gate` 改读它，不再因为某个 rotate 或维护 slot 迟到就挡住 dbt。

**退役**：`oi-gap-heal` slot / `oi_gap_heal` handler / `option_oi_extract.py` / `scripts/backfill_oi.py` / CronJob / Dagster 资产与调度全部删除——OI 现在随链快照按 session 写入，没有 gap 可补。**`vendor_gap_fix` 保留**：P3 原文列了它，但它是 `stock_daily_grouped` 的别名、与 OI 模型无关，且 `bifrost-trade-api` 的数据就绪页在调用，删它只会弄坏 Trade 的页面。

**P3 收尾（0.14.0）**：三项补齐。① 分页可续跑——`_paginate` 支持从 vendor cursor 起跑，超过 `max_pages` 时把剩余部分作为带 cursor 的续跑 job 入队（深度上限 20）；payload 只存 cursor token，不存 vendor URL。② 保留期按交易会话计（`option_snapshot_keep_sessions: 90`），假期与长周末不再悄悄缩短窗口。③ 新增收盘护栏：EOD job 若目标是今天且尚未收盘（16:00 NY），返回 `skipped: session_open`——盘中数据不得盖上收盘锚点，那和 P3 修掉的是同一类谎。

**存储影响**：修好后每会话存全部约 115,618 个活跃合约（原来只有约 40% 落对位置），约 35MB/会话、90 天保留约 2.1GB。vendor 调用量不变——这些合约本来每次就全量下载了，只是存错了地方。

### P4 挖掘（0.15.0 / Research 0.93.0，Owner 批准回填 A + 日内 A）

**Owner 决策**：期权回填 = watchlist ∪ SPY/QQQ/IWM/SPX ∪ M7，2 年，行权价 ±30%、每合约最多定价最后 90 天；日内链快照 10:30 / 13:00 / 15:30 ET，日内行保留 30 天（EOD 仍 90 个会话）。

- **股票日线回填**：`stock_daily_grouped` 按交易日各一个 job，2021-09-01 → 2025-05-31 共 978 个。分区扩到 5 年（y2021–y2027）。**最早 6 天（2021-09-01～09-08）返回「past historical entitlements」**——Stocks Starter 是 5 年滚动窗口，今天的边界正好是 2021-09-09，这不是缺陷；doctor 认得这类错误，不会去重试。滚动窗口意味着最老的一天每天都在过期。
- **期权历史回填**：新 kind `option_backfill_plan`，按「标的 × 到期月」切分（26 标的 × 24 月 = 624 个计划 job）。实测 SPY / SPX 两年内各有 5 万+ 合约，单个请求走不完，所以计划器只负责枚举一个月、套用 ±30% / DTE≤90 过滤、批量入队 `option_daily`。行权价基准取合约定价窗口起点当天的标的收盘价——**因此必须等股票回填完成再跑**，否则拿不到 2025-06 之前的现价，过滤会失效。取不到现价时保留合约而不是丢弃。
- **日内链快照**：复用 P3 的观测时间模型（payload 带 `intraday` + `observed_at`），所以日内行与 16:00 的 EOD 行共存而不抢主键。trim 按「不等于 16:00 NY」识别日内行，给它们单独的 30 天时钟。Dagster 三个调度走 **America/New_York** 时区，固定 UTC cron 会随夏令时漂一小时。
- **国债收益率**：新表 `raw_market.treasury_yield`（7 个期限，`/fed/v1/treasury-yields`，任何套餐免费）。已回填 2021-09-01 起 1,251 行。Research 的期权模型此前把无风险利率写死。
- **回填执行状态（2026-09-08 收盘）**：股票日线 972/978 完成（6 个是 5 年滚动窗口边界），`stock_daily` = 2021-09-09 → 2026-09-04、1,362 万行 / 20,691 标的，窗口内 935 个交易日对 972 个工作日，差额 37 天正是休市日，无空洞。期权计划器 673 个完成，枚举 134.2 万个合约、按 ±30% / DTE≤90 保留 113.1 万个；`option_daily` 已达 2024-09-09 → 2026-09-04（24 个月跨度打满）、63.3 万行 / 25 标的，队列剩约 82 万个任务在加密广度，按实测约 12 次/秒需约 19 小时。**实际规模是我最初估算（约 35 万次调用）的 2.4 倍**——成本是时间不是钱（套餐不限调用）。
- **回填过程中修掉的四个缺陷**（三个是 doctor 主动报的）：
  1. **健康端点在负载下说谎**（0.15.1）：`/health` 与 worker 事件循环共用线程，而 handler 同步写库，一次 12,000 行的批量写就让 stocks 池完全不应答、看起来像宕机。已移到独立线程并新增 `loop_lag_sec`；doctor 现在把滞后的池报成「饱和」而非「宕机」。修复后实测 0.01 秒应答、如实报 47 秒滞后。
  2. **SPY 计划器语句超时**（0.15.1）：单条语句插入数千行撞上超时，改为 500 一批。
  3. **SPX 枚举恒为 0**（0.15.2）：计划器把快照端点的 `I:SPX` 传给了合约参考端点，后者要的是 `SPX`，导致 SPX 全部 24 个月枚举不到任何合约。
  4. **指数没有现价、过滤失效**（0.15.3）：`stock_daily` 里没有指数点位（需要 Indices 套餐），SPX 因此保留全部行权价——24 个月 274,414 个合约，占整个回填队列三分之一。`IndexOptionSpec` 新增 `spot_proxy`，用 SPY×10 代理 SPX（误差远小于 1%，在 ±30% 带宽里可忽略）。修复后滤掉 6.5%——指数行权价本就密集分布在现价附近，这个比例是真实的。
- **99 个旧月份计划器跑早了**：它们在股票现价回填到那段之前执行，保留了全部行权价。是多拉不是少拉；已在现价就绪后重跑（`no_spot_reference` 归零，如 TSLA 920 个合约滤掉 504 个）。
- **Research staging 契约修正**：`stg_ratios` 原来是硬编码的空表（注释说 vendor 没有 ratios 报表——自从插件按日全市场拉取后就不成立了，实测 2026-09-04 有 4,791/4,797 个标的带 `return_on_equity`）。`stg_short_volume` / `stg_short_interest` 读的是 camelCase 键，payload 里从来没有，所以每一列都是 null；真实键是 snake_case，且 `short_volume_ratio` 是百分数而模型契约写的是比例。`mart_sepa_fundamental_ext` 把 ratios 过滤成 `period_type='quarterly'`，而 ratios 是按日的，匹配不到任何行。`short_pct_float` 保持 null 并写明原因：流通股数不在当前订阅内（float 端点 404），依赖它的两个 SEPA 情绪信号因此静默。

### P4 验收准备（2026-09-08 晚，队列仍在排空）

跑验收指标时又抓到一个静默缺陷，并测出两条订阅事实。

**缺陷：全市场做空数据只拉了六成（0.15.5 已修）**。`short-interest` 用 45 天回溯，一次返回多个结算日、每个约 1.5 万行，合计 4 万多行，撞上 30 页上限后**在字母 PARAW 处截断**，Q 到 Z 完全没有——而 job 仍报成功。这就是 SEPA 情绪维度只覆盖 68.2% 的原因。页上限提到 120，并且所有全市场 handler 现在遇到 `truncated` 直接抛错：静默存 60% 的市场比一个红任务糟糕得多，红任务 doctor 能处理。修复后 2026-08-14 结算日的标的数从截断的 15,045 涨到 22,477，覆盖 A → ZZHGY。

**订阅事实：`ratios?date=D` 忽略日期参数**。实测请求 2026-08-20 返回的仍是 2026-09-04 的值。所以比率**历史无法回填**，只能靠每日 slot 向前累积；我为测试排的 10 个历史日期任务全是空转。P4 的「Financials & Ratios 全量」由 P2 起就在跑的每日全市场拉取满足，不需要回填。

**当前验收测量**（dbt 已用修正后的 staging 重跑）：

| 指标 | 修复前 | 现在 | 验收线 |
|---|---|---|---|
| SEPA 情绪维度 shares_short / days_to_cover | 0%（键名全错）→ 68.2%（截断） | **99.2%** | ≥ 80% ✅ |
| SEPA 情绪维度 short_volume_ratio | 0% | **97.1%** | ≥ 80% ✅ |
| SEPA 比率维度覆盖 | 0%（空表） | **74.8%** | ≥ 80% ❌ |
| `stg_ratios` 行数 | 0 | 5,036（99.9% 带 roe） | — |
| `stg_short_volume` 有效比值 | 0 / 240 | 15,398 / 15,398 | — |
| `stg_short_interest` 有效做空股数 | 0 / 236 | 30,231 / 30,231 | — |

比率维度的 74.8% **是 vendor 的天花板，不是缺陷**：其比率端点只覆盖 5,017 个标的，SEPA 宇宙有 5,376 个，缺的 1,353 个全部是普通股（小盘股与新上市，vendor 没有为它们计算财务比率）。两个口径本身就不重合。要提高只有两条路：收窄 SEPA 宇宙到「vendor 有比率的标的」，或升级订阅。

`short_pct_float` 仍恒为 null——流通股数不在当前订阅内（float 端点 404），依赖它的两个情绪信号静默。

### 2026-09-27 巡检修复（Plugin 0.54.0 / 0.55.0 · Research 0.145.0）

巡检时 doctor 判 healthy（0 critical / 0 warning），coverage 页有四处 partial / thin，逐项分因后处理：

| 发现 | 原因 | 处理 |
|---|---|---|
| ratios 总是晚两个 slot 才落库 | `ratios?date=` 忽略日期、只给 vendor 最新值；vendor 在会话次日才发布，04:30 UTC 周二–六那一轮问得太早（周五的 ratios 周日下午已有，下一轮在周二） | 新 slot `ratios-market`（每 3 小时、含周末，只拉 ratios）；Research 新增 `market_ratios_market_schedule`（`10 2-20/3 * * *` UTC）并进 husbandry 白名单。`fundamentals-market` 保留 ratios 作为 doctor 的底线 |
| `option_daily` 09-09 只有 26 个标的（相邻日约 560） | 那天 `option-bars` 仍只续 watchlist，09-10 才扩到宇宙 | `enqueue-slot option-bars date=2026-09-09 force` 重放，91,950 个 job、0 失败 → 36,877 行 / 652 个标的 |
| `option_daily` 08-31～09-04 被标 thin | **不是缺陷**：这几天来自 P4 回填（每合约只定价到期前 90 天），越靠近回填终点 09-04 远月合约越少；按标的看比现行日更阶梯（约 126 行/标的）还多。coverage 拿回填最密的 08-26～08-28 作邻居才显得薄 | 不补。09-10 后新进宇宙的约 230 个标的在 09-08 前没有期权日线，属 `option-backfill` 的深度问题，另议 |
| `option_minute` 18/26 | benchmark-only 档 = watchlist ∪ IV-radar 基准，但 `minute-bars` 只走 watchlist，SPY QQQ IWM SPX AAPL MSFT AMZN META 从未有分钟线（`option-bars` 09-10 已修过同类问题） | 0.55.0：`minute-bars` 改走并集；SPX 只进期权轮转（SPY 代理现价），不拉股票分钟线（需 Indices）；期权批量 80 → 120，保持每个名字约 5 天轮一次 |
| `polygon-ws-ingestor` 心跳每 34 秒 WRONGPASS | 2026-08-19 轮换密码时 `install-redis-massive.sh` 只重建了 ACL，没动 ingestor 自己读的 `redis-massive-ws-secret` | 从 `.env` 重写 Secret 并重启，`bifrost:health:ws_massive_option` 恢复刷新（`ws_mode=rest_only`）；脚本改为同时写两份 Secret |

### 2026-09-27 防复发（Plugin 0.56.0 · Research 0.146.0）

上表两处期权日线缺口的共同根因是「没有东西会主动发现并补上」，补数据之外加了两道机制：

| 缺口 | 为什么当时没人发现 | 机制 |
|---|---|---|
| 09-10 后新进宇宙的 134 个标的没有 09-08 以前的期权日线 | `option-backfill` 是 Owner 手动一次性运行，没有调度；重跑会把 650 个名字的两年全部重拉，所以没人敢重跑 | 新 slot **`option-depth`**：按每个标的最早一根 `option_daily` 判断是否达到分层深度（resident/core 24 个月、edge 12 个月，宽限 30 天），只给不达标的名字规划、且只规划最早 bar 之前还能补到的到期月份；读不到最早日期时整轮跳过，不退化成全量。Research `market_option_depth_schedule` 每周日 07:30 UTC 触发。宇宙达标后每轮接近 0 个作业 |
| 09-09 只有 26 个标的（相邻日约 340） | doctor 连续性检查只问「这天有没有行」，有行就算通过；coverage 的 thin 是只读指标 | 契约新增 `refill_narrow`（stock_daily / short_volume / option_daily）。doctor 对这些表按**每日标的数**与前 10 个交易日中位数比较，低于一半即开出与缺失日相同的补数处方（共用每轮 3 天上限、先旧后新），00:45 UTC 的 `market_self_heal` 自动执行；当天正在写入的会话不判。按标的数而非行数：08-31～09-04 标的数与邻居相同、只是合约少（回填 DTE 边缘），按行数会每晚误开处方 |

首轮 `option-depth` 手动触发（2026-09-27 22:25 UTC）：134 个标的、2,511 个规划作业，展开约 15.8 万个合约作业。已知代价：天然历史较短的标的（新上市）每周会重拉最早几个月，约 30 个名字，周日空闲时段内消化。

### 2026-09-27 兜底加固（Plugin 0.57.0 · Research 0.147.0 · Console · infra monitoring）

起因：Ingest 面板 Daily Volume 只有最近三天有柱子。**数据没有丢**——面板读的是 `job_ingest`，而 trim 只保留 48 小时的已完成行，caption 却写着 ~7d。评估兜底链时另找出五处缺口，Owner 选择全部处理：

| 缺口 | 处理 |
|---|---|
| Daily Volume 只看得到 48 小时 | `/ingest/history` 的 done / failed 改读 `ops_jobs.queue_sample`（保留 90 天，按 `sample_ts - 1s` 归日，因为样本覆盖的是前一个区间），pending / running 仍读 live 行；`days` 上限 90，Console 加 90d 选项并改正 caption。断档表示 sampler 停了，不表示没有运行 |
| 部分丢失判不出来：ratios 09-21 / 09-22、08-11 只落了一部分，doctor 仍判 ok（绝对底线太低） | `fundamentals-market` 的 ratios / short_volume 底线改为 `max(绝对底线, 前 10 个会话中位数 × 0.9)`，不足 3 个会话时退回绝对底线；落库不足判 warn 并开 `fundamentals-market` 重拉处方（vendor 仍在提供这一期时有效） |
| `refill_narrow` 的「低于邻居一半」太宽 | `continuity.narrow_days`：低于前 10 日中位数 × 0.85，且（有后续日时）低于后 10 日最大值 × 0.85 才判窄——回填边缘那种逐日递减的形态不误报 |
| 自愈只在 00:45 UTC 跑一次，早于 fundamentals（04:30） | Research 新增 `market_self_heal_late_schedule`（`30 5 * * 2-6` UTC），同一个 doctor → heal → recheck 作业，进 husbandry 白名单 |
| 没有 market-data 告警，doctor 结论只在打开页面时可见 | Plugin `GET /metrics`（读 doctor 缓存，不另查库）：`bifrost_market_data_doctor_findings{severity}`、每条 crit / warn 的 `bifrost_market_data_doctor_finding{id,slot,severity,title}`、报告时间戳、处方数。infra `k8s/monitoring/bifrost-market-data.yaml` ServiceMonitor + 规则组 `bifrost-market-data`：Critical（3h）、WarningLingering（26h，即跨过两次自愈仍未消除）、DoctorStale、ScrapeDown、WorkersDown |
| KLAC 期权深度规划每月 0 个合约 | `option_backfill_plan` 用今天的拆股调整现价筛 ±30% 行权价带，而拆股前到期的合约行权价未调整（KLAC 2026-06-12 1 拆 10，拆股前到期的合约对应的 spot 只剩约十分之一；ORLY 2025-06 1 拆 15、IBKR 1 拆 4、ETR 1 拆 2 同类），全部落在带外。现按 `corporate_action` 把 spot 反调到到期日口径。实测 KLAC 2024-10～2025-09 每月保留 42–98 个合约（原为 0），共 906 个作业 |

上线核对（2026-09-27 23:50 UTC 后）：Prometheus target `market-data-api` up，5 条规则 loaded，指标与 doctor 一致（0 crit / 3 warn：`option_daily` 08-24 / 08-25 / 08-26 被新 `narrow_days` 判窄）。这三天已手动执行处方，约 27.6 万个 `option_daily` 作业（按每 30 分钟约 2 万个排空）；08-27～09-04 的其余窄日由 00:45 / 05:30 两轮自愈按每轮 3 天上限、先旧后新陆续补。根因是 P4 回填终点与 09-10 `option-bars` 扩到全宇宙之间的空档，每个标的只有到期前 90 天的合约。

hotfix ConfigMap 盘点（只盘点，撤不撤由 Owner 决定）：

| ConfigMap | 挂载 | 与 repo 的差异 |
|---|---|---|
| `plugin-market-data/market-data-api-schema-hotfix`（`deps.py`，08-25） | 无 | 孤儿，可删 |
| `plugin-market-data/market-data-quality-hotfix`（`quality.py`，09-05） | 无 | 孤儿，可删 |
| `api-monitor-status-hotfix`（dev / stg / prod） | api-monitor 覆盖 `status.py`、core `monitor/reader/common.py`、`portfolio/reader/accounts.py` | `status.py`：dev / stg 已与 repo 一致，prod 差 88 行；`common.py` 缺 `get_short_option_legs`；`accounts.py` 差 160 行，缺 core 0.19.0 期权成本口径修复与 0.18.2 的 Golden `raw_broker` 目标 |
| `bifrost-core-accounts-hotfix`（dev / prod） | api-account 覆盖 `accounts.py` | 差 23 行，同样缺 0.19.0 修复 |

也就是说，后两个 hotfix 把镜像里更新的代码压回了旧版本。

Owner 决定（2026-09-28）：两个插件孤儿已删；DEV（api-monitor、api-account）与 STG（api-monitor）撤掉挂载并删除 ConfigMap——先在 `:stg` 镜像里核对三个文件与 repo HEAD 的 sha256 一致（core 0.25.0），撤后 `/health`、`/status` 200，三个环境持仓数与字段一致。**PROD 的两个 hotfix 随 Owner 下一次 PROD 发布一起撤**（上文 `status.py` 替换同批）。

PROD 发布（2026-09-28 01:10 UTC）：先撤 `bifrost-prod` 的两个挂载并删 ConfigMap（撤后 `/health` 200、`/status` 持仓数不变），删掉因旧路径失败的 `db-init-prod` Job，再跑 `bifrost-deliver-prod`（17 task）与 `bifrost-deliver-platform-prod`（10 task），全部成功。核对：Argo `bifrost-prod` / `bifrost-platform-prod` Synced Healthy；core 0.25.0；`/status` 无 `polygon_ws` / `redis_massive`；`db-init-prod` 成功；daemon 副本数与 observe-safe 不变；PROD 新 role token 各得其角色，旧占位值与 dev 默认值均未认证。集群里已无任何 hotfix ConfigMap。随后执行了上面的运行时删除清单（先确认 0 引用），DEV / STG / PROD `api-market` 与插件 API 均 200。

### 2026-09-28 数据状态复核

面板显示 Draining 的含义：`husbandry` 在 `schedule=on_plan`（无 missed / due 槽）且队列有活跃作业时就是 draining，本身不是故障。

- `option_daily` 08-24～08-26 已补满（约 655 个标的）。08-27～09-04 七个窄日（约 425 个）原计划等自愈，但自愈 cron 是周二至周六，周一 00:45 不跑，每轮又只开 3 个处方，要到周三才补完；已直接对七天执行 `enqueue-slot option-bars force`，约 62.6 万个作业。
- 新发现：`option-depth` 只看每个标的**最早**一根 bar 够不够深，中间整月为空看不见（coverage `at_target` 同样按跨度）。实测 11 个标的有中段空月：BKNG 11、FISV 9、KLAC 8（2025-10～2026-05）、NOW 6、FAST / MNST / NFLX 各 4、B 3、CVNA / IBKR 各 2、AXTI 1。前 8 个是拆股（空月都在拆股前，规划早于 0.57.0 的行权价还原修复，之后没有重新规划）；B、FISV 是改代码。已对空月及其后 3 个到期月入队 100 个 `option_backfill_plan`，派生约 6.3 万个作业；AXTI 那个月 24 个合约全在 ±30% 带外，属实际无数据。
- 预防（Plugin 0.58.0，Owner 选「两处都修」）：`plan_option_depth` 由 `option-depth` 槽与 doctor 共用。槽除了原来的「最早 bar 不够深」，也规划最早 bar 之后的整月空洞（每个空月规划它和其后 `dte` 天内的到期月）；每个规划过的到期月在 `ops_jobs.symbol_source_void` 记一行 `option_plan:YYYY-MM`，30 天内每周例行与自愈都不重复拉取，`force` 例外。doctor 新增 `depth_holes:option_daily`：有未规划空月即 warn，处方是 `option-depth` 槽；已规划仍空的月份（如 AXTI）只写进 ok 的说明，不成为常驻告警。空月读法是逐月常量边界语句（分区在计划期裁剪，650 名 24 个月约 0.2 s；`generate_series` 连接写法不能裁剪，实测 35 s）。上线后首轮：doctor 找出 14 个标的（多于上面手工统计的 11 个），执行一次 `option-depth` 规划 52 个标的共 682 个到期月（含近年上市、深度不足的名字），复查 `depth_holes` 为 ok：60 个空月全部已规划。

### 2026-09-28 浅日与 IV 历史（Plugin 0.59.0 · Research 0.148.0，Owner 选「重算并预防」）

- **浅日**：08-17～08-21 每天有约 650 个标的，和前后一样，所以按「不同标的数」判的窄日看不见；但其中约 230 个只有当周到期的合约，Research 的 ATM IV（到期 5～90 天）没法给它们定价，这五天 ATM IV 只有约 390 个标的。已对五天执行 `enqueue-slot option-bars force`（约 44.9 万个作业）。
- **预防（插件 0.59.0）**：`DatasetContract.narrow_filter`；`option_daily` 的窄日广度只数有 `(expiry - bar_date) BETWEEN 5 AND 90` 的行（与 `iv_solver.DTE_MIN..DTE_MAX` 一致）。主库实测 81 天 3.7 s；上线后 doctor 判出 10 个窄日（08-17～21、08-31～09-04），09-14～18（比值约 0.9）不判。自愈的 force 处方与已排队作业按 payload 去重。
- **IV 历史滞后**：波动率槽每天只重算 3 个交易日、周日回填 90 天，插件晚于这个窗口补进来的 raw 不会进 ATM IV / 分位数 / VRP。实测 2025-03-12：raw 588 个标的、ATM IV 487 个，重算后 546 个；两年 500 个交易日里 439 个低于 raw 的 0.85。另外 09-27 22:00 的周日回填 run 被 00:07 的 Research 0.147.0 发布打断成孤儿（写了 max pain，没写 ATM IV），已终止。
- **预防（Research 0.148.0）**：`engines/volatility/iv_coverage_heal.py`，挂在周日 `vol_weekly_backfill` 之后。逐日比较「有 ATM IV 的标的数」与「raw 有 5～90 天到期合约的标的数」，低于 0.85 且 raw 在特征之后写过的交易日，重算 ATM IV + PCR；再从最早一天起逐日重算 IV 分位数与 VRP（两者都按前几天已存的值排名）。重算后仍低的交易日不再重复处理。
- **一次性全量重算**：队列排空后以 `research-iv-history-repair` Job 跑 `--derive-only --apply`（ATM IV → 分位数 → VRP → fwd_ret_20d → canonical PnL，逐日从最老到最新）。

## 3. Owner 待决事项

1. ~~期权回填范围~~ — 已定（2026-09-08，回填 A，见 P4）。
2. ~~`option_snapshot` 主键改造~~ — 已定（2026-09-08，方案 A，见 P3）。
3. SEPA 宇宙是否收敛到 "vendor 有报表的 CS"（约 4,465 只），其余进 void 登记。
4. ~~日内快照节奏与保留期~~ — 已定（2026-09-08，日内 A：10:30 / 13:00 / 15:30 ET，日内行 30 天）。
5. 退役清单确认：option-trades、~~ws-ingestor~~、`trades-quotes` / `filings` 路由、`stock_financials` 旧表、Option Discovery 流动性面板的 vendor 调用。（ws-ingestor 已定：2026-09-27 连同 redis-massive 一起退役、删代码，见下节。）

### ws-ingestor / redis-massive 退役（Owner 2026-09-27）

Options Starter 没有实时 WS，`polygon-ws-ingestor` 常驻只写一个 `ws_mode=rest_only` 心跳（`bifrost:health:ws_massive_option`），全仓没有任何代码读 redis-massive 里的 `massive:*` 报价；redis-massive 唯一的写入方就是它。Owner 选择两者一起退役、代码直接删除（git 历史保留，将来升级订阅再恢复）。

- Plugin：删 `src/bifrost_market_data/ws/`、`scripts/run_polygon_ws.py`、`k8s/base/deployment-polygon-ws.yaml`、`k8s/redis-massive/`、`k8s/external-names/`、`install-redis-massive.sh` / `apply-external-names-massive.sh`、configmap 的 `polygon_ws` / `redis_massive` / `watchlist_pg` 块、`websockets` 依赖。
- 下游同批：trade-core（0.25.0，删 massive Redis URL / 心跳键 / `socket_massive_disconnected`）、trade-api（`/status` 不再有 `socket.polygon_ws`）、trade-frontend（状态灯与拓扑节点）、bifrost-platform（卫星总线组件、Critical Processes、sessions-catalog、Tier B `massive-ws-quotes`）、bifrost-trade-infra（Trade 配置 `redis_massive`、`REDIS_MASSIVE_*` secrets、verify-phase-b、NetworkPolicy、platform overlay sessions-catalog）。
- 顺序：心跳 TTL 180 秒，消费端在读不到时会判红，所以先发代码与消费端，最后才删集群里的 Deployment、redis-massive、ExternalName 与 Secret（`kubectl apply -k` 不 prune，删掉 manifest 不会让线上立即消失）。

进度（2026-09-27）：六个 repo 已推 main；Platform STG 与 Trade STG 已发布（DEV 同用 `:stg` 镜像，api-monitor 已重启），`/status` 与卫星总线都不再有 polygon_ws。**PROD 随 Owner 下一次正常发布带上**，之后执行运行时删除。

PROD 发布时必须先做：三个环境的 api-monitor 都挂着手工 ConfigMap `api-monitor-status-hotfix`（2026-08-25，不在任何 repo），用它覆盖镜像里的 `status.py` 与 core 的 `common.py` / `accounts.py`。旧 `status.py` 会 import core 0.25.0 已删的名字，新 Pod 起不来（STG 首次发布即因此卡在 rollout）。STG / DEV 已把其中 `status.py` 换成 repo 版本；PROD 发布前对 `bifrost-prod` 做同样的替换。另两个文件与 repo 分别差 9 / 160 行，即三个环境一直跑着 08-25 版的 core 读取代码 —— 独立问题，待定是否撤掉这个 hotfix。

PROD 发布之后的运行时删除清单：

```
kubectl -n plugin-market-data delete deploy/polygon-ws-ingestor secret/redis-massive-ws-secret svc/redis-massive
kubectl -n data delete deploy/redis-massive svc/redis-massive networkpolicy/redis-massive-ingress secret/redis-massive-acl
kubectl -n bifrost-dev delete svc/redis-massive
kubectl -n bifrost-stg delete svc/redis-massive
kubectl -n bifrost-prod delete svc/redis-massive
```

`bifrost-*-secrets` 里残留的 `REDIS_MASSIVE_*` 键已无人读取，下次重新生成 Secret 时自然消失。

深度回填结果：158,084 个合约作业、0 失败；不达深度的标的 134 → 31（另 13 个没有任何期权日线，多为无挂牌期权的 edge 小盘）。余下 31 个多为近两年上市 / 分拆 / 改代码（HONA、FDXF、CBRS、FIG、CRCL、CRWV、XYZ、PSKY…）或拆股后合约改名（ORLY、IBKR、ETR），属已知代价；KLAC 最早只到 2025-06，原因是拆股后的行权价带错位，0.57.0 已修（见「兜底加固」一节）。

---

## 4. 发布方式

Plugin 不在 Argo 下：推 GitHub → `make k3s-sync-gitea-mirrors`（infra）→ `kubectl -n cicd create -f bifrost-trade-infra/k8s/cicd/tekton/pipelinerun-build-market-data.yaml`（改 image tag）→ 确认 registry 有 tag → `kubectl apply -k k8s/base` → `make verify-market-data`。

上线后先看 `GET /api/v1/plugins/market-data/api/market/doctor`：它就是这一版的验收面。
