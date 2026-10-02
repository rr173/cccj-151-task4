# 离线车队回执中继（Fleet Receipt Relay）

撤离编排系统的**独立配套模块**：离线保存不可变批次执行包供现场终端下载，接收车队
「已离场 / 未能出发 / 到达确认」回执，并在网络不稳定下**按序、恰好一次**地回调主系统。

- **零第三方依赖**：Python 3.9+ 标准库（`http.server` + `sqlite3` WAL），原生 HTML/JS 页面。
- **独立部署**：独立进程、端口（8090）、SQLite 文件与容器；与主系统只通过
  **出站执行包**与**回调接口**相连。
- **不可变执行包**：内容哈希存档；完全相同的再次导入返回原结果，
  同 `batch_ref` 内容不同一律拒绝。
- **严格顺序**：每批次单调序号 + 持久游标；乱序回执缺口缓冲，补齐后连续转发。
- **恰好一次**：两阶段租约（dispatching/lease）+ 稳定幂等键；
  `forwarded` 永不重调；回调超时/5xx 指数退避安全重试。
- **终态拒绝不越过**：主系统返回「批次已撤回 / 车辆已变化 / 目的地已变化」时，
  该序号冻结为 `rejected_terminal`，游标不动，其后回执永久缓冲；
  人工确认终态解除后才可重投，全程审计。
- **冲突拒绝**：同一序号或同一回执编号但内容不同 → 409 拒绝且不推进游标。
- **崩溃恢复**：重启自动回收过期租约、恢复待转发队列继续处理，已成功回调零重复。
- **可替换适配器**：内置 `http`（真实主系统）与 `stub`（离线演示/测试），
  替换只需实现 `CallbackAdapter.deliver()`。

## 快速开始

### 方式一：容器（含主系统，推荐）

```bash
cd relay
docker compose up -d --build
# 主系统  http://localhost:8080
# 中继    http://localhost:8090/relay/
```

中继默认使用 HTTP 适配器回调 `http://evac:8080/api/relay-callback`；
两个服务各自挂载独立命名卷（`evac-data` / `relay-data`）。

### 方式二：本地直接运行（离线演示，桩适配器）

```bash
cd relay
./run.sh                 # 自动播种演示批次；仅需 python3
# 打开 http://localhost:8090/relay/
```

接真实主系统：

```bash
RELAY_CALLBACK_ADAPTER=http \
RELAY_CALLBACK_URL=http://127.0.0.1:8080/api/relay-callback \
./run.sh
```

播种脚本（幂等）：

```bash
python3 seed_demo.py          # 3 个演示批次；DEMO-B 故意只有序号 1、3
python3 seed_demo.py --reset  # 清库重播
```

## 3 分钟演示（桩适配器）

1. **执行包导入**：点「填入示例包」→「导入」；再次点导入 → 返回 `replay:true`；
   改一个字段再导入 → 409 不可变冲突。
2. **回执上传**：「填入乱序示例(1,3)」上传 → 总览页该批次显示
   缺口 `[2]`、序号 3 处于缓冲；序号 1 已转发。
3. 再上传序号 2（`failed_to_depart`，回执号 `R-1002`）→ 2、3 连续转发，缺口清空。
4. 重复上传序号 1 的原文 → `duplicate_replay`，适配器调用数不增加（恰好一次）；
   用同序号不同内容 → 409 `conflict_seq`，游标不动。
5. **桩适配器**页给 `B-DEMO-1:1` 注入 `timeout`（或 `http_503`）→
   「失败重试」页可见退避与尝试次数；注入 `ack` 后点「立即重试」恢复。
6. 给某批次序号注入 `batch_withdrawn` / `vehicle_changed` /
   `destination_changed` → 批次变红 **终态阻塞**，后续序号全部缓冲无法越过；
   在「失败重试」页确认终态解除后重投，队列恢复。
7. 杀掉进程再 `./run.sh`：队列、缺口、终态阻塞全部恢复；已成功回调不重复。

## 页面

- **批次总览**：每批次缺口、缓冲项、各状态计数、终态阻塞项；卡片点开逐条回执
  的转发结果与失败原因，2 秒自动刷新。
- **执行包导入**：JSON 导入、幂等/冲突结果回显、一键示例。
- **现场终端下载**：清单（含内容哈希）与不可变包下载。
- **回执上传**：批量 JSON 上传与逐条入账结果；一键乱序示例。
- **失败重试**：待转发/退避/终态队列，立即重试与「确认终态解除并重投」。
- **审计与冲突**：全量审计流（派发、退避、终态、恢复、人工操作）与冲突留痕。
- **桩适配器**：规则注入与**适配器调用记录**（直接验证恰好一次）。

## 与主系统的连接契约

```
编排系统                         中继                           现场终端
   │  GET /api/execution-         │                              │
   │  package/v1/{batch_id}  ────►│ 不可变存档(packages)          │
   │                              │ GET /terminal/manifest  ◄────►│
   │                              │ GET /terminal/packages/{ref} ►│
   │                              │                              │
   │                              │◄── 回执上传 seq+receipt_no+摘要
   │  POST /api/relay-callback ◄──┤ 严格按序、租约+退避转发        │
   │  ack / rejected(终态原因) ───►│                              │
```

主系统侧（本仓库 `backend/relay_link.py`，已接入）：
- `GET /api/execution-packages?status=scheduled` — 可导出批次列表。
- `GET /api/execution-package/v1/{batch_id}` — 执行包（首导快照车辆/目的地）。
- `POST /api/relay-callback` — 按 `idempotency_key` 幂等；
  返回 `batch_withdrawn`（批次不存在/已撤回）、`vehicle_changed`
  （导出后车辆被改派/故障下线）、`destination_changed`（改派/避难点暂停）；
  `departed` 回执联动既有「离场承诺」语义。回调台账 `relay_callbacks` 持久留痕。

替换其他主系统：实现自己的 HTTP 回调端点（返回上述 JSON 契约），
或在 `relay/backend/adapters.py` 继承 `CallbackAdapter`，
用 `RELAY_CALLBACK_ADAPTER` 选择。

## 目录结构

```
relay/
  backend/
    db.py          SQLite 表结构与连接（packages/cursors/receipts/conflicts/audit_log）
    core.py        导入/入账/冲突判定/视图/重试/启动恢复（全部事务逻辑）
    forwarder.py   后台转发器：租约两阶段、游标、连续转发、退避、终态冻结
    adapters.py    可替换回调适配器：stub / http
    server.py      HTTP 路由 + 页面托管
  frontend/        index.html styles.css app.js（无构建原生 SPA）
  tests/
    test_relay.py         中继端到端单元测试（16 项）
    integration_test.py   中继×主系统双进程集成测试（13 项）
  Dockerfile docker-compose.yml run.sh seed_demo.py API.md README.md
```

## 测试

```bash
python3 relay/tests/test_relay.py        # 16 项：不可变/幂等/乱序/冲突/
                                         # 退避重试/终态冻结/重启恢复/恰好一次
python3 relay/tests/integration_test.py  # 13 项：真实双进程 + HTTP 适配器，
                                         # 导出→导入→回调→离场承诺→撤回终态→重启
```

主系统原有测试不受影响（`tests/planner_test.py` 14、`e2e_test.py` 48、
`branch_test.py` 45）。

## 一致性说明

- HTTP 线程与转发线程的所有读写在单个 `RLock` 内串行；网络 I/O 在锁外进行，
  以「领取租约 → 锁外回调 → 锁内 CAS 终局」保证不会双发。
- 游标只在**游标所在序号**获得 ack 时 +1；终态拒绝不修改游标，从状态机层面
  杜绝后续回执越过失败项。
- 批量上传为单事务：逐条写入不提交，任一条冲突整体回滚（仅保留拒绝审计）。
- SQLite WAL 保证崩溃一致性；重启时 `dispatching` 过期租约回收为 `queued`，
  `forwarded` 永不回收。
