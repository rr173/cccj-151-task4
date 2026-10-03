# 离线车队回执中继 —— API

独立模块，仅依赖 Python 标准库。默认挂载前缀 `/relay`（页面 `/relay/`，接口 `/relay/api/...`）。
所有写操作在全局锁内串行；后台转发器共用同一把锁。

## 顺序与恰好一次模型

- 每个批次独立游标 `cursors.next_seq`（初始为执行包 `seq_base`，通常 1）。
- 回执状态机：
  `queued`（就绪）/`buffered`（缺口缓冲）/`dispatching`（回调中，带租约）
  /`forwarded`（终态成功，永不重调）/`retry_wait`（瞬时失败退避中）
  /`rejected_terminal`（终态拒绝，**冻结游标**）。
- 序号大于游标的回执**持久缓冲**；缺口补齐后，缓冲项自动转 queued 并连续转发。
- 转发幂等键：`{batch_ref}:{seq}:{内容哈希前16位}`，回调重试/重发放送同一键。
- `dispatching` 带 30s 租约（`RELAY_LEASE_SECONDS`）；进程崩溃后重启回收过期租约为 queued。
  `forwarded` 行永不参与回收 → 已成功回调绝不重复调用。

## 管理/操作 API

### 执行包导入（不可变、幂等）
`POST /relay/api/packages/import`
```json
{"package": {"batch_ref":"B-001","batch_summary":"...","seq_base":1,
             "vehicle":{...},"destination":{...},"items":[...]}}
```
- 201 首次存档；200 `replay:true` 为**完全相同**内容的再次导入（返回原结果）。
- 同 `batch_ref` 内容不同 → **409 package_conflict**，原包永不覆盖，记冲突。
- 必需字段：`batch_ref`、`batch_summary`；`seq_base` 默认 1。

### 批次视图
- `GET /relay/api/batches` — 批次卡片：`cursor`、`gaps[]`、`buffered_items[]`、
  各状态计数、`blocker`（终态拒绝项）、`status`。
- `GET /relay/api/batches/{batch_ref}` — 详情：全部回执（状态、尝试次数、
  `result_code`/`result_detail`/`last_error`、时间）、执行包原文。

### 回执上传
- `POST /relay/api/receipts` — 单条：
```json
{"batch_ref":"B-001","seq":1,"receipt_no":"R-0001",
 "type":"departed|failed_to_depart|arrived","batch_summary":"...",
 "at":"2026-10-02T08:00:00Z"}
```
- `POST /relay/api/receipts/batch` 或
  `POST /relay/api/batches/{batch_ref}/receipts` — 批量：
```json
{"batch_ref":"B-001","batch_summary":"...",
 "receipts":[{...},{...}]}
```
  批量为**原子事务**：任一条被拒整批不生效（已插入的先回滚），仅留拒绝审计。

判定规则：

| 情况 | 结果 |
|---|---|
| 未知批次 | 404 `unknown_batch`，记冲突 |
| 批次已封存归档 | 409 `batch_archived`，记冲突；归档记录永不回到转发队列 |
| 批次摘要不匹配 | 409 `summary_mismatch`，记冲突 |
| 完全相同回执（按规范化内容哈希）再次提交 | 200 `duplicate_replay`，返回原结果 |
| 同 `seq` 内容不同 | 409 `conflict_seq`，**不推进游标** |
| 同 `receipt_no` 被不同内容/序号使用 | 409 `conflict_receipt_no` |
| `seq == next_seq` | `queued`，后台立即转发 |
| `seq > next_seq` | `buffered`，缺口补齐前不转发 |
| `seq < next_seq` 且无原记录 | 409 `seq_behind_cursor` |

### 队列 / 重试
- `GET /relay/api/queue` — 所有 queued/buffered/retry_wait/dispatching/终态拒绝行。
- `POST /relay/api/retries` `{"batch_ref","seq?","note?"}`
  — 把退避中的失败项立即重新入队；终态拒绝项返回 **409 terminal_requires_ack**。
- `POST /relay/api/batches/{ref}/receipts/{seq}/ack-terminal`
  `{"note":"..."}` — **人工确认主系统终态已解除**（批次恢复/车辆或目的地改回）
  后把终态拒绝项重投；全程审计，游标仍按序推进。

### 审计
- `GET /relay/api/audit` — 审计流：导入/重放/冲突/派发/成功/退避重试/终态拒绝/
  手动重试/租约回收/启动恢复/封存/校验/恢复。
- `GET /relay/api/conflicts` — 被拒冲突留痕（同序异容、同号异容、摘要不符、未知批次、
  已封存批次）。

## 归档封存 API（按批次回收磁盘空间）

只有**无序号缺口、无待转发/缓冲/派发/退避项、无终态阻塞**（即全部回执已终态成功）
的批次可以封存。封存把热库回执明细迁入只读归档表，释放热库空间；
写归档 + 删热库明细 + 删游标在**同一事务**完成，进程崩溃/重启不会留下半迁移状态
（启动恢复另有兜底清扫）。原审计时间线保留在 `audit_log`，归档包内另存快照。

- `GET /relay/api/archives/estimate` — 封存评估：每批次 `eligible`、`blockers[]`
  （缺口/待重试/阻塞原因）、`bytes_estimate`（待回收估计）、`audit_events`；
  汇总 `total_reclaimable_bytes` 与当前 `hot_receipt_bytes`。
- `POST /relay/api/archives/seal` `{"batch_ref":"B-001"}`
  — 201 首次封存：返回 `archive_id`、`manifest_hash`（内容清单校验摘要）、
  `receipt_count`、`cursor_final`、`bytes_reclaimed`。
  同一批次重复封存 → 200 `replay:true`，返回**第一次**结果。
  进行中批次 → 409 `batch_not_sealable`（附 `blockers`）；
  封存前逐条复核热库内容哈希，失败 → 409 `checksum_mismatch`，热库原样保留。
- `GET /relay/api/archives` — 归档列表（编号、摘要、计数、封存/恢复/校验时间）。
- `GET /relay/api/archives/{ref}` — 归档元信息 + 完整内容清单（manifest）。
- `GET /relay/api/archives/{ref}/download` — 下载归档包
  （清单 + 执行包原文 + 回执明细 + 审计快照），重复下载字节一致，
  响应头带 `X-Manifest-Hash`，可离线复核。
- `POST /relay/api/archives/{ref}/verify` — 按清单深度复核归档包
  （清单摘要/执行包哈希/逐条回执哈希/审计计数）；失败 → 409 `checksum_mismatch`。
- `POST /relay/api/archives/{ref}/restore` — **先校验再恢复只读详情**：
  校验失败拒绝；成功后 `GET /relay/api/batches/{ref}` 与
  `GET /relay/api/archives/{ref}/detail` 返回从归档包还原的只读明细
  （`read_only:true`）。恢复不写回热库，重复恢复返回首次状态（幂等）。
- `GET /relay/api/archives/{ref}/detail` — 只读归档详情；未恢复 → 409 `not_restored`。

封存后的行为：该批次回执一律 409 `batch_archived`；重试/终态确认同样拒绝；
游标已删除、转发器跳过归档批次，**归档记录永不再次进入转发队列**。

## 现场终端 API（只读）

- `GET /relay/api/terminal/manifest` — 可下载批次清单（含内容哈希与下载地址）。
- `GET /relay/api/terminal/packages/{batch_ref}` — 下载不可变执行包，
  内嵌 `relay.content_hash` 与 `immutable:true`，重复下载字节一致。

## 回调适配器契约

适配器 `deliver(package, receipt, idempotency_key)` 返回三态：

| 结果 | 含义 | 中继行为 |
|---|---|---|
| `ack` | 主系统受理 | forwarded，游标 +1，连续转发下一项 |
| `retry` | 超时/网络/408/429/5xx | retry_wait，指数退避（1→2→4…，上限 60s，可配置） |
| `terminal` | 批次已撤回 / 车辆或目的地已变化 | rejected_terminal，游标冻结 |

实现：
- `stub`（默认）：进程内桩，可按 `batch:seq`/`batch:*`/`*` 注入
  `ack|timeout|network_error|http_5xx|batch_withdrawn|vehicle_changed|
  destination_changed|fail_then_ok[:N]`。
- `http`：POST `RELAY_CALLBACK_URL`，携带 `X-Idempotency-Key`；
  主系统 200 `{"result":"ack"}` 为成功，
  `{"result":"rejected","reason":"batch_withdrawn|vehicle_changed|destination_changed"}`
  为终态拒绝。

## 运维 API
- `GET /relay/api/health` — 适配器类型、转发统计、启动回收数。
- `POST /relay/api/admin/stub` `{"rules":{...},"reset_calls":false}`
  — 注入桩规则（仅 stub 适配器）。
- `POST /relay/api/admin/state` — 适配器状态 + 桩最近调用记录（验证恰好一次）。
- `POST /relay/api/admin/recover` — 手动触发租约回收（启动时已自动执行）。

## 环境变量
`RELAY_PORT`(8090) `RELAY_HOST` `RELAY_DB`(./data/relay.db)
`RELAY_CALLBACK_ADAPTER`(stub|http) `RELAY_CALLBACK_URL`
`RELAY_CALLBACK_TIMEOUT`(8s) `RELAY_TICK_SECONDS`(1)
`RELAY_LEASE_SECONDS`(30) `RELAY_BACKOFF_BASE`(1) `RELAY_BACKOFF_CAP`(60)
