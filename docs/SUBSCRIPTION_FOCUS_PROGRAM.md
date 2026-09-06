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
| P2 收敛 | slot 按授权矩阵重排；新增 ratios / short 全市场日更；宇宙卫生 + 按 symbol 的 void；watchlist 缓存；重活出 API；Dagster RetryPolicy + 告警；未授权能力的占位说明（API / Console / Trade UI） | 🔄 代码完成，0.11.0 发布中 | 2026-09-06 |
| P3 模型 | `option_snapshot` 主键改观测时间；OI 从快照派生；job 幂等键带 session；健康判定按 session 完整性 | ⏳ 需 Owner 批准 DDL | — |
| P4 挖掘 | 5 年股票 / 2 年期权回填；日内链快照；Financials & Ratios 全量；国债收益率；Research staging 契约修正 | ⏳ | — |

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

### P1 验收

- Console market lane 连续 5 个交易日无误报
- 每日 vendor 请求 ≤ 4,500（此前约 7,400）
- EOD 期权窗口 ≤ 15 分钟（此前约 2 小时）
- `job_ingest` 每日失败行 = 0（不含 vendor 空结果）

### P1 未纳入（留给 P2）

- Research `market_corporate_trades` 资产仍会调用 `option-trades`，现在得到 `skipped: unentitled`，无害；随 P2 的 dbt 契约修正一起改 Dagster。
- Trade 前端 Option Discovery 的 last-trade / quotes 调用另行提交（前端仓库）。

---

## 3. Owner 待决事项

1. 期权回填范围：建议 watchlist ∪ SPY/QQQ/IWM/SPX ∪ M7，共 22 个标的，2 年，行权价 ±30%、DTE ≤ 90。
2. `option_snapshot` 主键改造（P3，架构级 DDL）。
3. SEPA 宇宙是否收敛到 "vendor 有报表的 CS"（约 4,465 只），其余进 void 登记。
4. 日内快照节奏与保留期：建议 10:30 / 13:00 / 15:30 ET，90 天。
5. 退役清单确认：option-trades、ws-ingestor、`trades-quotes` / `filings` 路由、`stock_financials` 旧表、Option Discovery 流动性面板的 vendor 调用。

---

## 4. 发布方式

Plugin 不在 Argo 下：推 GitHub → `make k3s-sync-gitea-mirrors`（infra）→ `kubectl -n cicd create -f bifrost-trade-infra/k8s/cicd/tekton/pipelinerun-build-market-data.yaml`（改 image tag）→ 确认 registry 有 tag → `kubectl apply -k k8s/base` → `make verify-market-data`。
