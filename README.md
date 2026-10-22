# DeepSimulate-Back

A 股高仿真模拟交易后端。Rust + Axum + Tokio + SQLite，行情来自本地 `tdx-api`（通达信数据服务）。

## 能力范围

- 真实五档与逐笔成交：行情任务每秒轮询 `tdx-api`，写入内存，不入库
- 模拟 L2：快照覆盖 + 逐笔扣减推算档位流动性
- 撮合：按股票代码分组，仅遍历受影响股票；吃单按对手档位价成交，限量于该档挂单量
- 排队：委托排在当前价位已有挂单量之后，逐笔成交消耗前方排队量
- T+1、涨跌停（普通 10% / ST 5%）、整手与零股、资金与持仓校验、佣金与印花税
- REST + WebSocket：行情与委托状态推送；断线重连由前端主动对齐 REST 快照
- SQLite 异步批量落盘：用户、账户、持仓、委托、成交、日终快照；实时行情不落库

## 运行

前置：`tdx-api` 已在 `127.0.0.1:8080` 监听（`tdx-api/web/server.exe`）。

```
cd DeepSimulate-Back
cargo run --release
```

启动序列：连接 SQLite → 恢复遗留活动委托（撤销并解冻）→ 加载股票代码表（名称与 ST 判定）→ 监听 `127.0.0.1:9010` → 启动行情任务。

环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DEEPSIM_BIND` | `127.0.0.1:9010` | 监听地址 |
| `DEEPSIM_TDX_BASE` | `http://127.0.0.1:8080` | 行情源地址 |
| `DEEPSIM_DB` | `sqlite://deepsim.db` | SQLite 连接串 |
| `DEEPSIM_INITIAL_CASH` | `1000000000` | 新用户初始资金，单位厘（=100 万元） |
| `DEEPSIM_QUOTE_MS` | `700` | 五档轮询间隔 |
| `DEEPSIM_TICK_MS` | `1000` | 逐笔轮询间隔 |
| `DEEPSIM_TICK_BATCH` | `4` | 每轮逐笔轮询的股票数（逐个接口，轮转） |
| `DEEPSIM_TICK_KEEP` | `120` | 前端逐笔列表保留笔数 |

`RUST_LOG=deepsimulate_back=debug` 打开详细日志。

## 模块

```
src/
  main.rs          启动、装配、跨日结算定时任务
  config.rs        环境变量配置
  model.rs         领域类型：价格/金额统一用厘(i64)，数量用股(i64)
  error.rs         thiserror 领域错误 + API 错误体
  market/
    mod.rs         内存行情态、订阅计数、轮询任务、逐笔增量比对
    client.rs      tdx-api HTTP 客户端（/api/quote /api/trade /api/minute /api/codes）
  engine/
    mod.rs         Engine：按代码分组的委托簿容器
    book.rs        撮合：吃单、排队消耗、撤单
    l2.rs          模拟 L2 盘口推算
    fees.rs        佣金/印花税
    validate.rs    委托前置校验（涨跌停、整手、资金、持仓）
    reject.rs      废单原因
  account/
    mod.rs         资金/持仓内存态、T+1 结算、冻结与解冻
  db/
    mod.rs         schema、读写、批量落盘线程
  api/
    mod.rs         路由装配、会话与鉴权提取器
    handlers.rs    REST 端点
    ws.rs          WebSocket 升级与订阅
  service.rs       Core：下单/撤单/成交入账/推送编排
  ws.rs            推送中心（按订阅广播行情，按用户推送委托）
```

## 单位约定

| 概念 | 单位 | 说明 |
| --- | --- | --- |
| 价格 | 厘 | 1 元 = 1000 厘，最小变动 10 厘（0.01 元） |
| 金额 | 厘 | 佣金/印花税按厘计算后四舍五入到分 |
| 数量 | 股 | 委托与持仓均为股；行情接口返回的挂单量为手，已在客户端换算 |
| 成交量 | 手 | 与 tdx-api 原始字段一致 |

## 撮合规则

1. 吃单（立即成交）：买单限价 ≥ 卖一，逐档吃卖盘；卖单限价 ≤ 买一，逐档吃买盘。成交价取**被吃档位价**，成交量不超过该档当前挂单量，剩余留在队列。
2. 排队：未立即成交的委托，`queue_ahead` = 委托价对应的当前档位挂单量。逐笔成交在该价位出现时先扣减 `queue_ahead`，归零后的剩余成交量才回补该委托。
3. 触发时机：仅在行情更新（新快照或新逐笔）时撮合，且只处理该股票；撮合路径不访问行情源。
4. 费用：佣金万分之 2.5，最低 5 元，四舍五入到分；印花税卖出千分之一。佣金按**同一委托累计成交额**计算后逐笔补差，因此多次部分成交只承担一次最低佣金。
5. 资金：买入冻结 = 限价 × 数量 + 佣金；成交时按实际成交金额与费用扣减，委托终结时退还未用部分。

## T+1

- 买入成交增加总持仓，不增加可卖数量。
- 以日期水位驱动结算：账户首次访问或每分钟定时任务检测到交易日变化时，可卖数量恢复为总持仓，并写入日终快照。
- 重启后水位来自 `accounts.last_settle`，当日买入不会因重启而解锁。

## 模拟 L2 的推算与局限

推算：以五档快照为基准，用逐笔成交的方向与数量扣减对应档位挂单量（主动卖出吃买盘、主动买入吃卖盘、中性单按价格就近选择），新快照到达时直接覆盖校准。

局限：

1. 五档只暴露总量，不暴露队列构成，无法还原同一价位上的真实委托分布；
2. 逐笔时间精度为交易所回报时间，同一时刻多笔只能按顺序扣减；
3. 逐笔价落在两档之间（快照已过期）时按最近档扣减，存在偏差，由下一次快照纠正；
4. 每笔用户委托独立维护前方排队量，不建模多笔自有委托之间的相互挤压；
5. 这不是真实 L2 队列，也不代表真实可成交量。

## REST

鉴权：`Authorization: Bearer <token>`。错误体统一为 `{ "code": "...", "message": "..." }`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/auth/register` | `{username,password}` → `{token,user_id,username}` |
| POST | `/api/auth/login` | 同上 |
| GET | `/api/quotes?codes=a,b` | 五档（缺失时即时补拉） |
| GET | `/api/quotes/{code}/minute` | 当日分时（含累计均价线） |
| GET | `/api/quotes/{code}/ticks` | 最近逐笔，最新在前 |
| GET | `/api/search?q=` | 按代码或名称搜索（5581 只） |
| GET | `/api/fee-estimate?side&price&qty` | 下单前费用预估 |
| GET | `/api/me/account` | 资金、持仓、盈亏 |
| GET | `/api/me/orders?limit=` | 历史委托 |
| GET | `/api/me/trades?limit=` | 成交明细（含佣金、印花税） |
| POST | `/api/orders` | `{code,side,price,qty}`；校验失败返回 200 + `status=rejected` 的委托 |
| DELETE | `/api/orders/{id}` | 撤单 |
| GET/POST/DELETE | `/api/watchlist` | 自选股 |
| GET | `/api/status` | 行情源状态、未完成委托数、各股票撮合耗时 |

## WebSocket

`ws://127.0.0.1:9010/ws?token=<token>`

客户端 → 服务端：`{"type":"subscribe","codes":["000001"]}`、`{"type":"unsubscribe","codes":[...]}`、`{"type":"ping"}`

服务端 → 客户端：

- `{"type":"ready","data":{"user_id","username"}}`
- `{"type":"quote","data":{...五档...}}`（按订阅广播）
- `{"type":"tick","data":{"code","ticks":[...]}}`（增量逐笔）
- `{"type":"order","data":{...}}`（仅本人）
- `{"type":"trade","data":{...}}`（仅本人）
- `{"type":"account","data":{...}}`（仅本人，成交后推送）

订阅只影响是否轮询该股票；取消全部订阅后行情任务空转等待。

## 数据库

`users`、`accounts`、`positions`、`orders`、`trades`、`daily_snapshots`、`watchlist`。

落盘为独立任务：账户/持仓/委托/成交事件经 `mpsc` 汇入，50ms 窗口按主键合并（同一实体只写最终值），单事务提交，避免撮合路径同步写盘。

重启恢复：内存委托簿为空，启动时把遗留 `pending/partial` 委托置为已撤、解冻资金与持仓，账户与持仓从库恢复。

## 测试

```
cargo test
```

覆盖：T+1 拒单、涨跌停废单、整手与零股、资金校验、吃单成交价、部分成交、排队消耗、快照触发、同价位 FIFO、撤单释放、佣金（最低 5 元 / 万分之 2.5 / 累计计费）、印花税、模拟 L2 扣减与覆盖校准。

联调脚本：

```
python scripts/e2e_smoke.py    # REST 全流程
python scripts/ws_probe.py     # WebSocket 订阅与推送
python scripts/load_test.py    # 50 并发下单
```

## 已知边界

- 不接真实委托通道，成交完全由本地撮合模拟；行情之外的任何资金变动都是虚拟的。
- 非交易时段行情源数据静止，逐笔不再增长，排队委托需等到下一次开盘才会推进。
- 单机内存态：多实例部署需要共享行情与撮合状态，当前未实现。
- 会话 token 存内存，进程重启后需重新登录（账户与持仓不受影响）。
