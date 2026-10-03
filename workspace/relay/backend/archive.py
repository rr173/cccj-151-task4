"""批次归档封存：把已完结批次的热库明细迁入只读归档，回收中继磁盘空间。

封存条件（全部满足才允许）：
  * 无序号缺口（gaps 为空）；
  * 无待转发/缓冲/派发中/退避重试项（queued/buffered/dispatching/retry_wait 为 0）；
  * 无终态阻塞项（rejected_terminal 为 0）；
  即：全部回执都已到达终态成功（forwarded）。

封存语义：
  * 生成内容清单(manifest)与校验摘要(manifest_hash = sha256)；
  * 归档包 = 清单 + 执行包原文 + 全部回执明细 + 该批次审计时间线快照；
  * 写归档、删热库明细、删游标在**同一事务**：进程崩溃/重启不会留下半迁移状态，
    启动恢复还会兜底清扫任何残留的已封存批次热库行；
  * 同一批次重复封存返回第一次结果（幂等）；
  * 封存前逐条复核热库内容哈希，校验失败拒绝封存；
  * 已封存批次拒收回执(409 batch_archived)、游标删除、转发器跳过，
    归档记录永不再次进入转发队列；
  * 原审计时间线保留在热库 audit_log（不删除），归档包内另存快照供离线下载。

所有函数假定调用方持有 db.lock（HTTP 线程与转发线程串行）。
"""
import json

import core
from core import RelayError, canonical, sha256_hex

ROW_OVERHEAD = 160          # 每行回执的索引/元数据估计字节（占用预估用）
ARCHIVE_FORMAT = "relay-archive/v1"
NON_FINAL = (("queued", "待转发"), ("buffered", "缺口缓冲"),
             ("dispatching", "转发中"), ("retry_wait", "退避重试"))


def _arch_row(db, batch_ref):
    return db.conn.execute(
        "SELECT * FROM archives WHERE batch_ref=?", (batch_ref,)).fetchone()


# ============================================================ 预估占用

def _eligibility(db, prow):
    """返回 (blockers, counts, next_seq)。blockers 为空即可封存。"""
    ref = prow["batch_ref"]
    counts = {r["state"]: r["c"] for r in db.conn.execute(
        "SELECT state,COUNT(*) c FROM receipts WHERE batch_ref=? GROUP BY state",
        (ref,))}
    cur = db.conn.execute("SELECT next_seq FROM cursors WHERE batch_ref=?",
                          (ref,)).fetchone()
    next_seq = cur["next_seq"] if cur else prow["seq_base"]
    max_seen = db.conn.execute(
        "SELECT MAX(seq) m FROM receipts WHERE batch_ref=?", (ref,)).fetchone()["m"]
    blockers = []
    if max_seen is not None:
        present = {x[0] for x in db.conn.execute(
            "SELECT seq FROM receipts WHERE batch_ref=?", (ref,))}
        gaps = [s for s in range(next_seq, max_seen + 1) if s not in present]
        if gaps:
            blockers.append({"kind": "gaps", "seqs": gaps,
                             "message": f"存在序号缺口 {gaps}"})
    for st, label in NON_FINAL:
        if counts.get(st):
            blockers.append({"kind": st, "count": counts[st],
                             "message": f"{counts[st]} 条{label}中"})
    if counts.get("rejected_terminal"):
        blockers.append({"kind": "rejected_terminal",
                         "count": counts["rejected_terminal"],
                         "message": f"{counts['rejected_terminal']} 条终态拒绝(阻塞)"})
    return blockers, counts, next_seq


def _estimate_row(db, prow):
    ref = prow["batch_ref"]
    blockers, counts, next_seq = _eligibility(db, prow)
    payload_bytes = db.conn.execute(
        "SELECT COALESCE(SUM(LENGTH(CAST(payload AS BLOB))),0) b FROM receipts"
        " WHERE batch_ref=?", (ref,)).fetchone()["b"]
    total_rows = sum(counts.values())
    audit_n = db.conn.execute(
        "SELECT COUNT(*) c FROM audit_log WHERE batch_ref=?", (ref,)).fetchone()["c"]
    if counts.get("rejected_terminal"):
        status = "blocked_terminal"
    elif counts.get("dispatching") or counts.get("retry_wait"):
        status = "forwarding"
    elif counts.get("buffered") or counts.get("queued"):
        status = "collecting"
    elif counts.get("forwarded"):
        status = "all_forwarded"
    else:
        status = "idle"
    return {
        "batch_ref": ref, "archived": False, "status": status,
        "eligible": not blockers, "blockers": blockers,
        "receipt_count": total_rows, "forwarded": counts.get("forwarded", 0),
        "cursor": next_seq,
        "bytes_estimate": payload_bytes + ROW_OVERHEAD * total_rows,
        "audit_events": audit_n,
    }


def estimate_all(db):
    """全库封存评估：每批次可封存性 + 待回收字节估计 + 当前热库占用。"""
    batches, total = [], 0
    for prow in db.conn.execute("SELECT * FROM packages ORDER BY imported_at"):
        arch = _arch_row(db, prow["batch_ref"])
        if arch:
            batches.append({"batch_ref": prow["batch_ref"], "archived": True,
                            "archive_id": arch["archive_id"],
                            "receipt_count": arch["receipt_count"],
                            "bytes_reclaimed": arch["bytes_estimate"],
                            "sealed_at": arch["sealed_at"],
                            "restored": arch["restored_at"] is not None})
            continue
        row = _estimate_row(db, prow)
        batches.append(row)
        if row["eligible"]:
            total += row["bytes_estimate"]
    hot = db.conn.execute(
        "SELECT COALESCE(SUM(LENGTH(CAST(payload AS BLOB))),0) b FROM receipts"
    ).fetchone()["b"]
    return {"batches": batches, "total_reclaimable_bytes": total,
            "hot_receipt_bytes": hot, "generated_at": core.now()}


# ============================================================ 封存

def _seal_result(arow, replay):
    return {"replay": replay, "batch_ref": arow["batch_ref"],
            "archive_id": arow["archive_id"], "manifest_hash": arow["manifest_hash"],
            "receipt_count": arow["receipt_count"], "audit_count": arow["audit_count"],
            "cursor_final": arow["cursor_final"],
            "bytes_reclaimed": arow["bytes_estimate"], "sealed_at": arow["sealed_at"],
            "message": ("同一批次已封存过，返回第一次封存结果" if replay else
                        "批次已封存：热库明细已迁入只读归档")}


def seal_batch(db, batch_ref, actor="api"):
    """封存一个已完结批次。幂等：重复封存返回第一次结果。

    拒绝：批次不存在(404) / 进行中(409 batch_not_sealable) /
    热库内容哈希校验失败(409 checksum_mismatch)。
    """
    batch_ref = str(batch_ref).strip()
    if not batch_ref:
        raise RelayError(400, "malformed", "缺少 batch_ref")
    prow = db.conn.execute("SELECT * FROM packages WHERE batch_ref=?",
                           (batch_ref,)).fetchone()
    if not prow:
        raise RelayError(404, "not_found", f"批次 {batch_ref} 不存在")

    existing = _arch_row(db, batch_ref)
    if existing:
        db.audit("batch_seal_replay", batch_ref, actor=actor,
                 detail={"archive_id": existing["archive_id"],
                         "manifest_hash": existing["manifest_hash"]})
        db.commit()
        return _seal_result(existing, replay=True)

    blockers, counts, next_seq = _eligibility(db, prow)
    if blockers:
        db.audit("batch_seal_rejected", batch_ref, actor=actor,
                 detail={"blockers": blockers})
        db.commit()
        raise RelayError(409, "batch_not_sealable",
                         "批次尚未完结，不能封存："
                         + "；".join(b["message"] for b in blockers),
                         {"blockers": blockers})

    # 封存前完整性复核：逐条重算热库内容哈希，校验失败拒绝封存
    rows = db.conn.execute(
        "SELECT * FROM receipts WHERE batch_ref=? ORDER BY seq", (batch_ref,)).fetchall()
    mismatched = []
    for r in rows:
        if sha256_hex(canonical(json.loads(r["payload"]))) != r["content_hash"]:
            mismatched.append({"seq": r["seq"], "receipt_no": r["receipt_no"]})
    if sha256_hex(canonical(json.loads(prow["payload"]))) != prow["content_hash"]:
        mismatched.append({"seq": None, "receipt_no": "", "package": True})
    if mismatched:
        db.audit("batch_seal_checksum_failed", batch_ref, actor=actor,
                 detail={"mismatched": mismatched})
        db.commit()
        raise RelayError(409, "checksum_mismatch",
                         "热库明细校验失败，拒绝封存（数据可能被篡改）",
                         {"mismatched": mismatched})

    # 内容清单 + 归档包（清单 / 执行包原文 / 回执明细 / 审计时间线快照）
    t = core.now()
    manifest_receipts, payload_receipts = [], []
    for r in rows:
        manifest_receipts.append({
            "seq": r["seq"], "receipt_no": r["receipt_no"],
            "type": r["receipt_type"], "content_hash": r["content_hash"],
            "attempts": r["attempts"], "result_code": r["result_code"],
            "forwarded_at": r["forwarded_at"]})
        payload_receipts.append({
            "seq": r["seq"], "receipt_no": r["receipt_no"],
            "receipt_type": r["receipt_type"], "content_hash": r["content_hash"],
            "state": r["state"], "attempts": r["attempts"],
            "result_code": r["result_code"], "result_detail": r["result_detail"],
            "created_at": r["created_at"], "updated_at": r["updated_at"],
            "forwarded_at": r["forwarded_at"],
            "payload": json.loads(r["payload"])})
    audit_rows = [dict(x) for x in db.conn.execute(
        "SELECT id,ts,kind,batch_ref,seq,receipt_no,actor,detail FROM audit_log"
        " WHERE batch_ref=? ORDER BY id", (batch_ref,))]
    bytes_est = sum(len(r["payload"].encode("utf-8")) for r in rows) \
        + ROW_OVERHEAD * len(rows)
    archive_id = f"ARC-{batch_ref}-{int(t * 1000)}"
    manifest = {
        "format": ARCHIVE_FORMAT, "archive_id": archive_id, "batch_ref": batch_ref,
        "package": {"content_hash": prow["content_hash"],
                    "package_id": prow["package_id"],
                    "batch_summary": prow["batch_summary"],
                    "seq_base": prow["seq_base"],
                    "imported_at": prow["imported_at"]},
        "cursor_final": next_seq,
        "receipt_count": len(rows), "receipts": manifest_receipts,
        "audit_count": len(audit_rows),
        "audit_span": [audit_rows[0]["ts"], audit_rows[-1]["ts"]] if audit_rows else None,
        "bytes_estimate": bytes_est,
        "sealed_at": t, "sealed_by": actor,
    }
    manifest_hash = sha256_hex(canonical(manifest))
    payload = {"format": ARCHIVE_FORMAT, "manifest": manifest,
               "manifest_hash": manifest_hash,
               "package": json.loads(prow["payload"]),
               "receipts": payload_receipts, "audit": audit_rows}

    # 单事务迁移：写归档 + 删热库明细 + 删游标。崩溃 → 整体回滚，不留半迁移状态。
    try:
        db.conn.execute(
            "INSERT INTO archives(batch_ref,archive_id,manifest_hash,manifest,payload,"
            "receipt_count,audit_count,cursor_final,bytes_estimate,sealed_at,sealed_by)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (batch_ref, archive_id, manifest_hash,
             json.dumps(manifest, ensure_ascii=False),
             json.dumps(payload, ensure_ascii=False),
             len(rows), len(audit_rows), next_seq, bytes_est, t, actor))
        db.conn.execute("DELETE FROM receipts WHERE batch_ref=?", (batch_ref,))
        db.conn.execute("DELETE FROM cursors WHERE batch_ref=?", (batch_ref,))
        db.audit("batch_sealed", batch_ref, actor=actor,
                 detail={"archive_id": archive_id, "manifest_hash": manifest_hash,
                         "receipt_count": len(rows), "audit_count": len(audit_rows),
                         "cursor_final": next_seq, "bytes_reclaimed": bytes_est})
        db.commit()
    except Exception:
        db.conn.rollback()
        raise
    return _seal_result(_arch_row(db, batch_ref), replay=False)


# ============================================================ 校验 / 恢复

def _verify_payload(arow):
    """按内容清单深度复核归档包。返回 checks 字典（全 True 才通过）。"""
    checks = {"manifest_hash": False, "package_hash": False,
              "receipt_hashes": False, "audit_count": False}
    try:
        payload = json.loads(arow["payload"])
        manifest = payload["manifest"]
    except (ValueError, KeyError, TypeError):
        return checks
    checks["manifest_hash"] = \
        sha256_hex(canonical(manifest)) == arow["manifest_hash"]
    pkg_raw = payload.get("package")
    pkg_meta = manifest.get("package", {}) if isinstance(manifest, dict) else {}
    checks["package_hash"] = isinstance(pkg_raw, dict) and \
        sha256_hex(canonical(pkg_raw)) == pkg_meta.get("content_hash")
    entries = {e.get("seq"): e for e in manifest.get("receipts", [])
               if isinstance(e, dict)}
    recs = payload.get("receipts", [])
    ok = isinstance(recs, list) \
        and len(recs) == manifest.get("receipt_count") == len(entries)
    if ok:
        for rec in recs:
            e = entries.get(rec.get("seq"))
            if not e or e.get("receipt_no") != rec.get("receipt_no") \
                    or e.get("content_hash") != rec.get("content_hash") \
                    or e.get("content_hash") != sha256_hex(
                        canonical(rec.get("payload"))):
                ok = False
                break
    checks["receipt_hashes"] = ok
    checks["audit_count"] = \
        len(payload.get("audit", [])) == manifest.get("audit_count")
    return checks


def verify_archive(db, batch_ref, actor="api"):
    """校验归档完整性。校验失败 → 409 checksum_mismatch（拒绝）。"""
    arow = _arch_row(db, batch_ref)
    if not arow:
        raise RelayError(404, "archive_not_found", f"批次 {batch_ref} 没有归档")
    checks = _verify_payload(arow)
    ok = all(checks.values())
    t = core.now()
    db.conn.execute(
        "UPDATE archives SET last_verify_at=?,last_verify_ok=? WHERE batch_ref=?",
        (t, 1 if ok else 0, batch_ref))
    db.audit("archive_verified", batch_ref, actor=actor,
             detail={"ok": ok, "checks": checks, "archive_id": arow["archive_id"]})
    db.commit()
    if not ok:
        raise RelayError(409, "checksum_mismatch",
                         "归档校验失败：归档包与内容清单校验摘要不一致",
                         {"checks": checks})
    return {"batch_ref": batch_ref, "ok": True, "checks": checks,
            "archive_id": arow["archive_id"], "manifest_hash": arow["manifest_hash"],
            "verified_at": t}


def restore_archive(db, batch_ref, actor="api"):
    """校验并恢复只读详情。校验失败拒绝；重复恢复返回首次状态（幂等）。

    恢复只影响归档元数据（restored_at），明细始终从只读归档包读取，
    不写回热库 —— 归档记录不会因此重新进入转发队列。
    """
    arow = _arch_row(db, batch_ref)
    if not arow:
        raise RelayError(404, "archive_not_found", f"批次 {batch_ref} 没有归档")
    verify_archive(db, batch_ref, actor=actor)  # 校验失败直接 409，不恢复
    replay = arow["restored_at"] is not None
    t = core.now()
    if not replay:
        db.conn.execute("UPDATE archives SET restored_at=? WHERE batch_ref=?",
                        (t, batch_ref))
    db.audit("archive_restored", batch_ref, actor=actor,
             detail={"archive_id": arow["archive_id"], "replay": replay})
    db.commit()
    return {"batch_ref": batch_ref, "restored": True, "replay": replay,
            "restored_at": arow["restored_at"] or t,
            "message": ("只读详情此前已恢复，返回原状态" if replay else
                        "只读详情已恢复（明细来自只读归档，不会回到转发队列）"),
            "detail": archived_batch_detail(db, batch_ref)}


# ============================================================ 视图

def archived_batch_detail(db, batch_ref, require_restored=False):
    """已封存批次的详情视图。

    未恢复：只返回归档元信息（热库明细已迁出）；
    已恢复：从只读归档包还原完整明细（回执、执行包、审计快照），read_only=True。
    """
    arow = _arch_row(db, batch_ref)
    if not arow:
        raise RelayError(404, "archive_not_found", f"批次 {batch_ref} 没有归档")
    if require_restored and not arow["restored_at"]:
        raise RelayError(409, "not_restored",
                         "归档尚未恢复：请先执行「校验并恢复只读详情」")
    prow = db.conn.execute("SELECT * FROM packages WHERE batch_ref=?",
                           (batch_ref,)).fetchone()
    restored = arow["restored_at"] is not None
    view = {
        "batch_ref": batch_ref, "archived": True, "read_only": True,
        "restored": restored,
        "archive_id": arow["archive_id"], "manifest_hash": arow["manifest_hash"],
        "sealed_at": arow["sealed_at"], "sealed_by": arow["sealed_by"],
        "restored_at": arow["restored_at"],
        "last_verify_at": arow["last_verify_at"],
        "last_verify_ok": (None if arow["last_verify_ok"] is None
                           else bool(arow["last_verify_ok"])),
        "package_id": prow["package_id"], "batch_summary": prow["batch_summary"],
        "content_hash": prow["content_hash"], "seq_base": prow["seq_base"],
        "imported_at": prow["imported_at"], "source": prow["source"],
        "cursor": arow["cursor_final"], "max_seen": None, "gaps": [],
        "buffered_items": [], "in_flight": [], "blocker": None,
        "status": "archived",
        "counts": {"buffered": 0, "queued": 0, "dispatching": 0, "retry_wait": 0,
                   "forwarded": arow["receipt_count"], "rejected_terminal": 0},
        "bytes_reclaimed": arow["bytes_estimate"],
    }
    if not restored:
        view["receipts"] = []
        view["message"] = ("批次已封存，热库明细已迁入只读归档；如需在线查看明细，"
                           "请先在归档页「校验并恢复只读详情」；"
                           "原审计时间线在「审计与冲突」页完整保留")
        return view
    payload = json.loads(arow["payload"])
    receipts = []
    for rec in payload["receipts"]:
        receipts.append({
            "seq": rec["seq"], "receipt_no": rec["receipt_no"],
            "type": rec["receipt_type"],
            "type_label": core.TYPE_LABELS.get(rec["receipt_type"],
                                               rec["receipt_type"]),
            "state": rec["state"],
            "state_label": core.STATE_LABELS.get(rec["state"], rec["state"]),
            "attempts": rec["attempts"], "last_error": "",
            "result_code": rec["result_code"],
            "result_detail": rec["result_detail"],
            "next_retry_at": None, "lease_until": None,
            "created_at": rec["created_at"], "updated_at": rec["updated_at"],
            "forwarded_at": rec["forwarded_at"], "rejected_at": None})
    view["receipts"] = receipts
    view["payload"] = payload["package"]
    view["audit_snapshot"] = payload["audit"]
    view["message"] = "只读归档详情（从封存包恢复，不会回到转发队列）"
    return view


def list_archives(db):
    out = []
    for a in db.conn.execute("SELECT * FROM archives ORDER BY sealed_at DESC"):
        out.append({
            "batch_ref": a["batch_ref"], "archive_id": a["archive_id"],
            "manifest_hash": a["manifest_hash"],
            "receipt_count": a["receipt_count"], "audit_count": a["audit_count"],
            "cursor_final": a["cursor_final"],
            "bytes_estimate": a["bytes_estimate"],
            "sealed_at": a["sealed_at"], "sealed_by": a["sealed_by"],
            "restored_at": a["restored_at"],
            "last_verify_at": a["last_verify_at"],
            "last_verify_ok": (None if a["last_verify_ok"] is None
                               else bool(a["last_verify_ok"])),
            "download": f"/relay/api/archives/{a['batch_ref']}/download"})
    return out


def get_archive_meta(db, batch_ref):
    arow = _arch_row(db, batch_ref)
    if not arow:
        raise RelayError(404, "archive_not_found", f"批次 {batch_ref} 没有归档")
    meta = list_archives(db)
    meta = next(m for m in meta if m["batch_ref"] == batch_ref)
    meta["manifest"] = json.loads(arow["manifest"])
    return meta


def get_archive_download(db, batch_ref, actor="api"):
    """返回 (archive_id, manifest_hash, payload_text)。重复下载字节一致。"""
    arow = _arch_row(db, batch_ref)
    if not arow:
        raise RelayError(404, "archive_not_found", f"批次 {batch_ref} 没有归档")
    db.audit("archive_downloaded", batch_ref, actor=actor,
             detail={"archive_id": arow["archive_id"]})
    db.commit()
    return arow["archive_id"], arow["manifest_hash"], arow["payload"]


# ============================================================ 启动兜底

def sweep_archived_hot_rows(db):
    """启动兜底清理：已封存批次不应残留任何热库明细/游标。

    正常封存是单事务，不会残留；此处防御性清扫（例如库文件被外部改动），
    保证归档记录在任何情况下都不会重新进入转发队列。
    """
    rcpts = db.conn.execute(
        "DELETE FROM receipts WHERE batch_ref IN (SELECT batch_ref FROM archives)"
    ).rowcount
    curs = db.conn.execute(
        "DELETE FROM cursors WHERE batch_ref IN (SELECT batch_ref FROM archives)"
    ).rowcount
    if rcpts or curs:
        db.audit("archive_sweep",
                 detail={"receipts_removed": rcpts, "cursors_removed": curs})
    return {"receipts_removed": rcpts, "cursors_removed": curs}
