---
version: 1.0
updated: 2026-09-09
status: 目标态 · 三轴（广度 / 深度 / 新鲜度）
---

# Massive 蓝图

> **这是应该的样子，不是现在的样子。** 现状在《Massive 校准》。
> 同一份文件：仓库 `bifrost-platform-plugin-market-data/src/bifrost_market_data/docs/MASSIVE_BLUEPRINT.md`，
> API `GET /market/docs/blueprint`。
> 契约在本文件定义，状态只记在校准里。本文件不出现任何状态符号。

## 1. 这个插件是干什么的

**持有我们付费买到的数据，深到计划允许的程度，新到本 session 要求的程度，并且随时能证明这三件事——不需要任何人去跑一条查询。**

它不是一个"跑任务的东西"。任务是手段，持有的数据才是产物。这个区分不是措辞讲究：只要度量的单位是任务，一个每天报成功却什么都没采的 slot 就是不可见的。

## 2. 三个轴

一个数据集的健康只能用三个问题描述，缺一个都会漏掉一整类故障。**每个轴都必须有分母，分母是契约的一部分，不是面板临时决定的。**

| 轴 | 问题 | 分母 | 它单独看不见什么 |
|---|---|---|---|
| **广度** | 我持有多少个标的 | 该数据集所属档位的口径（§3） | 每个标的只有三天历史 |
| **深度** | 每个标的往回有多久 | 订阅窗口，或该 tier 声明的要求 | 数据停在上周 |
| **新鲜度** | 本 session 的到了吗 | 该数据集声明的截止时间 | 只覆盖 26 个标的 |

三个轴互相独立：广度满而深度浅，是"刚开始采"；深度足而新鲜度差，是"停更了"；新鲜度好而广度窄，是"只盯着几个名字"。**任何把三者压成一个"健康分"的做法都会把这三种完全不同的处境显示成同一个颜色。**

## 3. 数据集契约表

**这是本蓝图的核心产物。** 每个数据集在这里声明它的三个目标；系统里任何地方要用到分母、窗口或截止时间，都从这里取。

### 3.1 三个档位

广度的分母按数据集的采集成本分档，而不是全系统一个口径：

| 档位 | 规则 | 分母 |
|---|---|---|
| **whole-market** | vendor 提供全市场端点，一次调用覆盖所有标的，边际成本近零 → 拉满订阅口径 | `raw_market.ticker` 中 `active` 的行 |
| **universe** | 必须按标的逐个调用，成本随标的数线性增长 → 跟随 Research 的宇宙规则 | `research.option_universe`（三层：resident / core / edge） |
| **benchmark-only** | 数据量极大且只有少数名字有分析价值 | `scheduler.iv_radar_benchmarks` |

档位是**业务决定**，不是技术限制。一个数据集从 benchmark-only 升到 universe，意味着 Owner 判断它值那份采集成本。

### 3.2 契约表

| 数据集 | 档位 | 深度目标 | 新鲜度截止 | 拥有它的 slot |
|---|---|---|---|---|
| `stock_daily` | whole-market | 滚动 5 年（订阅上限） | session 收盘后 2 小时 | `universe-daily`（全市场 grouped）+ `stock-eod`（宇宙补齐） |
| `stock_snapshot` | whole-market | 仅当期（point-in-time，无历史语义） | session 收盘后 1 小时 | `stock-snapshot` |
| `stock_movers` | whole-market | 仅当期 | session 收盘后 1 小时 | `stock-movers` |
| `ticker` | whole-market | 仅当期（含 `active` 退市标记） | 48 小时 | `reference` |
| `corporate_action` | whole-market | 前后窗口 −7 / +60 天 | 48 小时 | `corporate` |
| `income_statement` / `balance_sheet` / `cash_flow` | whole-market | 2009 年起（订阅上限） | 48 小时 | `fundamentals-rotate` |
| `ratios` / `short_volume` / `short_interest` | whole-market | **只能向前累积**（见 §4 C-D3） | 次日 04:30 UTC 后 | `fundamentals-market` |
| `treasury_yield` | whole-market | 滚动 30 天 | 每交易日 12:00 UTC | `treasury` |
| `us_market_holiday` | whole-market | 未来日历 | 48 小时 | `calendar` |
| `option_snapshot` | universe | 保留 90 个 session（trim 策略） | session 收盘后 2 小时 | `eod-pipeline` + `intraday-chain`×3 |
| `option_contract` / `option_expiration` | universe | 目录，随宇宙同步 | 12 小时 | `option-refresh` |
| `option_open_interest` | universe | 跟随 `option_snapshot` | 同 `option_snapshot` | 由 `eod-pipeline` 派生 |
| `option_daily` | universe | resident/core **24 个月**、edge **12 个月**（订阅上限 2 年） | 当期由 `option-bars`，历史由 `option-backfill` | `option-bars` + `option-backfill` |
| `stock_minute` / `option_minute` | benchmark-only | 滚动 12 个月 | 次日 | `minute-bars` |

**没有出现在这张表里的数据集，就是不该被采集的数据集。** 反过来，表里的每一行都必须能回答三个轴——不能回答的那一格是缺口，不是"不适用"。

## 4. 契约

### 广度

| 编号 | 契约 |
|---|---|
| **C-B1** | 每个数据集在契约表里声明档位与分母。系统中不存在第二个分母：任何面板、检查或报告要问"应该有多少标的"，只能从这张表取。 |
| **C-B2** | 广度同时报**意图达成率**（实有 ÷ 该档位口径）与**口径利用率**（该档位口径 ÷ 订阅允许的全部）。两个数都可见，永不合并成一个百分比——前者是运维问题，后者是策略问题。 |
| **C-B3** | 一个标的的缺席必须可归因，且三种归因语义不同：**vendor 无此数据**（如无期权链的股票）、**未排程**（能采但没有 slot 在采）、**采集失败**（排了但没成）。把三者混成"缺口"会让第一种永远像故障、第二种永远看不见。 |

### 深度

| 编号 | 契约 |
|---|---|
| **C-D1** | 每个数据集声明历史窗口目标，取自订阅口径（stocks 5 年 / options 2 年 / financials 2009 年起）或该 tier 的要求（24 / 12 个月）。目标写在契约表里，不写在代码常量里。 |
| **C-D2** | 深度按**标的**度量并可汇总：达标标的数、深度中位数、最浅的是谁。一个"全库最早日期"不是深度——只要有一个标的有五年数据，它就会显示五年。 |
| **C-D3** | **计划边界不是缺陷。** 滚动窗口的自然过期（5 年前的那一天今天到期）、vendor 结构性不支持的回填（ratios 端点忽略 `date` 参数，历史只能向前累积）——这些必须显式表达为边界，不得显示为缺口。反之，能补而没补的必须显示为缺口。 |
| **C-D4** | 回填进度可见：正在买的是哪个数据集的深度、还差多少标的、按当前消化速率还要多久。一个跑了三天的回填必须能在一屏里说清它买到了什么。 |

### 新鲜度

| 编号 | 契约 |
|---|---|
| **C-F1** | **"当期 session"全系统只有一个定义。** 它由交易日历与一个收盘后的锚点时刻决定，所有检查、面板与报告读同一个函数。 |
| **C-F2** | 每个数据集在契约表里声明自己的截止时间。过了截止才算迟到——一个次日 04:30 UTC 才发布的数据集，在当晚被判为"缺失"是误报。 |
| **C-F3** | 陈旧阈值只在一处定义，所有消费者读它。同一个数据集在两个面板上显示不同的健康状态，是这条契约被违反的直接证据。 |
| **C-F4** | **跳过不是成功。** slot 的依从性对**数据**判定，不对 job 状态判定：一个返回 `skipped` 的 slot，只有在它跳过的那一份数据确实已经存在时才算按计划完成。 |

### 治理

| 编号 | 契约 |
|---|---|
| **C-G1** | 契约表是唯一事实源。任何面板不得自造分母、阈值或 session 定义；结构上永远等于 100% 的分母（如 `max(期望, 实际)`）视同没有分母。 |
| **C-G2** | 每个数据集都要能回答三个轴。缺一个轴是缺口，需要在校准里记录并给出最小改动，不能标成"不适用"。 |
| **C-G3** | **entitled 但未持有的能力必须可见**，并标注它能解锁什么下游能力。付了钱而没在采的数据面，如果不显示出来，就永远不会被决定。 |

## 5. 达成后的样子

一屏之内可以回答五个问题，不需要任何人写查询：

1. 我付费买了什么（订阅口径与各自的窗口）
2. 持有了其中多少（按档位分别给出意图达成率与口径利用率）
3. 深到哪一年（每个数据集的达标标的数与深度中位数，以及正在回填的进度）
4. 今天的到了没有（按各自截止时间判定，不是一个统一的 24 小时）
5. 没到的是谁的责任（vendor 无数据 / 未排程 / 采集失败 / 计划边界）

而且——**一个每天报成功却什么都没采的 slot，在这一屏上是红的。**

## 6. entitled 但未开采（待决，不是目标）

以下数据面在当前订阅内、端点实测可用，但没有 handler、没有落库表、没有 slot。**它们不自动进入目标形态**：采不采由 Owner 逐项决定，本节只记录它们存在以及各自能解锁什么。

| 数据面 | 能解锁什么 |
|---|---|
| 新闻（`/v2/reference/news`） | 蓝图 §3.2 的事件面从"未测"变为可回测；Research 目前没有任何前瞻事件源 |
| 技术指标（`/v1/indicators`） | 免去自算 SMA/EMA/RSI/MACD；与 vendor 口径一致 |
| ticker events / IPO | 上市、更名、退市的时间线；回测里的幸存者偏差修正 |
| `open_close` / `prev_agg` | 单日精确开收盘的独立校验源 |
| `conditions` / `exchanges` | 成交条件与交易所字典；逐笔数据升级后才有意义 |
| `ticker_type` | 品种类型字典（表已建，handler 已有，无 slot） |
| 按 symbol 的除权除息 | 全市场窗口只覆盖 −7/+60 天，更早的历史无补采路径 |
| 按 symbol 的 ratios / short_* | 全市场按日累积可用，但单个标的的完整历史序列没有补采路径 |

## 7. 不在范围内

- **订阅升级才能拿到的**：期权逐笔（Options Developer）、期权报价、指数（Indices Starter）。它们是 *planned*，不是缺口——在《Massive 校准》里按能力记账，不按覆盖率记账。
- **vendor 已下线的**：`/stocks/v1/float`、`/stocks/filings/*`（404）。
- **调度本身**：Plugin 的 CronJob 全部挂起，排程归 Research 的 Dagster。本蓝图约束"应该持有什么"，不约束"谁来触发"。
