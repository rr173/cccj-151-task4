"""中继批次归档封存 —— 端到端测试（Python 标准库 unittest）。

覆盖：
  1. 预估占用：可封存/受阻批次判定与字节估计
  2. 封存成功：内容清单+校验摘要生成、热库明细迁入只读归档、游标删除、审计保留
  3. 重复封存返回第一次结果（幂等）
  4. 进行中批次（缺口/缓冲/退避重试/终态阻塞）拒绝封存且热库不动
  5. 热库校验失败拒绝封存，不留半迁移状态
  6. 已封存批次拒收回执、禁止重试，归档记录不再进入转发队列
  7. 下载归档字节一致，清单哈希可独立复核
  8. 校验接口发现篡改并拒绝恢复
  9. 校验并恢复只读详情（恢复前不可见、恢复后只读、重复恢复幂等）
 10. 重启后归档完整无半迁移；启动兜底清扫伪造的半迁移残留
 11. 原审计时间线保留（热库审计流 + 归档包内快照）

运行：python3 relay/tests/archive_test.py
"""
import json
import os
import sqlite3
import sys
import time
import unittest
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "backend"))

import core  # noqa: E402
from test_relay import (RelayServerHarness, batch_upload, package,  # noqa: E402
                        receipt, SUMMARY)


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.h = RelayServerHarness()
        self.h.call("POST", "/packages/import", {"package": package()})
        self.h.configure({"*": "ack"})

    def tearDown(self):
        self.h.stop()

    # ---- 工具 ----
    def forward_all(self, ref, seqs, summary=SUMMARY):
        """上传 seqs 并等待全部 forwarded。"""
        self.h.call("POST", "/receipts/batch", batch_upload(ref, [
            receipt(s, f"{ref}-R{s}", "departed" if s % 2 else "arrived", summary)
            for s in seqs], summary))
        last = max(seqs)
        self.h.wait_until(lambda: self.h.state(ref)["cursor"] == last + 1,
                          message=f"{ref} 全部转发到 {last + 1}")

    def db(self):
        return self.h.app.db

    def hot_rows(self, ref):
        with self.db().lock:
            return self.db().conn.execute(
                "SELECT COUNT(*) c FROM receipts WHERE batch_ref=?",
                (ref,)).fetchone()["c"]

    def seal_ok(self, ref):
        status, res = self.h.call("POST", "/archives/seal", {"batch_ref": ref})
        self.assertEqual(status, 201, res)
        return res

    # ---- 1. 预估占用 ----
    def test_estimate_eligibility_and_bytes(self):
        self.forward_all("B-001", [1, 2])
        # B-002：缺口 [2] + 缓冲 → 不可封存
        self.h.call("POST", "/packages/import", {"package": package("B-002")})
        self.h.call("POST", "/receipts/batch", batch_upload("B-002", [
            receipt(1, "B2-R1"), receipt(3, "B2-R3", "arrived")]))
        self.h.wait_until(lambda: self.h.state("B-002")["cursor"] == 2)
        # B-003：退避重试中 → 不可封存
        self.h.call("POST", "/packages/import", {"package": package("B-003")})
        self.h.configure({"B-003:1": "http_503"}, reset_calls=False)
        self.h.call("POST", "/receipts/batch",
                    batch_upload("B-003", [receipt(1, "B3-R1")]))
        self.h.wait_until(lambda: self.h.receipt_state("B-003", 1)[0]["state"]
                          == "retry_wait")

        _, est = self.h.call("GET", "/archives/estimate")
        by_ref = {b["batch_ref"]: b for b in est["batches"]}
        self.assertTrue(by_ref["B-001"]["eligible"])
        self.assertEqual(by_ref["B-001"]["blockers"], [])
        self.assertGreater(by_ref["B-001"]["bytes_estimate"], 0)
        self.assertEqual(by_ref["B-001"]["receipt_count"], 2)

        self.assertFalse(by_ref["B-002"]["eligible"])
        kinds = {b["kind"] for b in by_ref["B-002"]["blockers"]}
        self.assertIn("gaps", kinds)
        self.assertIn("buffered", kinds)

        self.assertFalse(by_ref["B-003"]["eligible"])
        kinds3 = {b["kind"] for b in by_ref["B-003"]["blockers"]}
        self.assertIn("retry_wait", kinds3)

        self.assertGreaterEqual(est["total_reclaimable_bytes"],
                                by_ref["B-001"]["bytes_estimate"])
        self.assertGreater(est["hot_receipt_bytes"], 0)

    # ---- 2. 封存成功：清单+摘要、热库迁出、审计保留 ----
    def test_seal_moves_hot_details_to_archive(self):
        self.forward_all("B-001", [1, 2])
        res = self.seal_ok("B-001")
        self.assertFalse(res["replay"])
        self.assertEqual(res["receipt_count"], 2)
        self.assertEqual(res["cursor_final"], 3)
        self.assertGreater(res["bytes_reclaimed"], 0)
        self.assertTrue(res["archive_id"].startswith("ARC-B-001-"))
        self.assertEqual(len(res["manifest_hash"]), 64)

        with self.db().lock:
            # 热库明细与游标已迁出
            self.assertEqual(self.hot_rows("B-001"), 0)
            self.assertIsNone(self.db().conn.execute(
                "SELECT * FROM cursors WHERE batch_ref='B-001'").fetchone())
            arch = self.db().conn.execute(
                "SELECT * FROM archives WHERE batch_ref='B-001'").fetchone()
            self.assertIsNotNone(arch)
            self.assertEqual(arch["manifest_hash"], res["manifest_hash"])
            self.assertEqual(arch["receipt_count"], 2)
            # 原审计时间线保留在热库
            aud = self.db().conn.execute(
                "SELECT COUNT(*) c FROM audit_log WHERE batch_ref='B-001'"
            ).fetchone()["c"]
            self.assertGreater(aud, 0)

        # 批次视图变为已封存（未恢复前无明细）
        _, d = self.h.call("GET", "/batches/B-001")
        self.assertTrue(d["archived"])
        self.assertEqual(d["status"], "archived")
        self.assertEqual(d["counts"]["forwarded"], 2)
        self.assertEqual(d["receipts"], [])
        _, lst = self.h.call("GET", "/batches")
        card = next(b for b in lst["batches"] if b["batch_ref"] == "B-001")
        self.assertEqual(card["status"], "archived")
        self.assertEqual(card["archive"]["archive_id"], res["archive_id"])

    # ---- 3. 重复封存返回第一次结果 ----
    def test_seal_idempotent_replay(self):
        self.forward_all("B-001", [1])
        first = self.seal_ok("B-001")
        status, second = self.h.call("POST", "/archives/seal",
                                     {"batch_ref": "B-001"})
        self.assertEqual(status, 200)
        self.assertTrue(second["replay"])
        for k in ("archive_id", "manifest_hash", "receipt_count",
                  "bytes_reclaimed", "sealed_at"):
            self.assertEqual(second[k], first[k])
        with self.db().lock:
            n = self.db().conn.execute(
                "SELECT COUNT(*) c FROM archives WHERE batch_ref='B-001'"
            ).fetchone()["c"]
            self.assertEqual(n, 1)

    # ---- 4. 进行中批次拒绝封存 ----
    def test_seal_rejected_when_in_progress(self):
        # 缺口 + 缓冲
        self.h.call("POST", "/receipts/batch", batch_upload("B-001", [
            receipt(1, "R-1"), receipt(3, "R-3", "arrived")]))
        self.h.wait_until(lambda: self.h.state("B-001")["cursor"] == 2)
        code, err = self.h.call("POST", "/archives/seal", {"batch_ref": "B-001"},
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "batch_not_sealable")
        kinds = {b["kind"] for b in err["blockers"]}
        self.assertIn("gaps", kinds)
        self.assertIn("buffered", kinds)
        self.assertEqual(self.hot_rows("B-001"), 2)  # 热库未动

        # 退避重试中
        self.h.call("POST", "/packages/import", {"package": package("B-002")})
        self.h.configure({"B-002:1": "timeout"}, reset_calls=False)
        self.h.call("POST", "/receipts/batch",
                    batch_upload("B-002", [receipt(1, "B2-R1")]))
        self.h.wait_until(lambda: self.h.receipt_state("B-002", 1)[0]["state"]
                          == "retry_wait")
        code, err = self.h.call("POST", "/archives/seal", {"batch_ref": "B-002"},
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "batch_not_sealable")

        # 终态阻塞
        self.h.call("POST", "/packages/import", {"package": package("B-003")})
        self.h.configure({"B-003:1": "batch_withdrawn"}, reset_calls=False)
        self.h.call("POST", "/receipts/batch",
                    batch_upload("B-003", [receipt(1, "B3-R1")]))
        self.h.wait_until(lambda: self.h.receipt_state("B-003", 1)[0]["state"]
                          == "rejected_terminal")
        code, err = self.h.call("POST", "/archives/seal", {"batch_ref": "B-003"},
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertIn("rejected_terminal",
                      {b["kind"] for b in err["blockers"]})

        # 不存在的批次
        code, err = self.h.call("POST", "/archives/seal", {"batch_ref": "B-NOPE"},
                                expect_error=True)
        self.assertEqual(code, 404)
        # 一个都没封存成功
        _, arch = self.h.call("GET", "/archives")
        self.assertEqual(arch["archives"], [])

    # ---- 5. 热库校验失败拒绝封存，不留半迁移状态 ----
    def test_seal_checksum_failure_rejected(self):
        self.forward_all("B-001", [1, 2])
        # 直接篡改热库回执原文（不更新内容哈希）
        with self.db().lock:
            self.db().conn.execute(
                "UPDATE receipts SET payload=? WHERE batch_ref='B-001' AND seq=1",
                (json.dumps({"seq": 1, "receipt_no": "R-FORGED",
                             "type": "arrived", "batch_summary": SUMMARY}),))
            self.db().commit()
        code, err = self.h.call("POST", "/archives/seal", {"batch_ref": "B-001"},
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "checksum_mismatch")
        self.assertEqual(err["mismatched"][0]["seq"], 1)
        # 不留半迁移状态：热库行还在、游标还在、无归档
        self.assertEqual(self.hot_rows("B-001"), 2)
        with self.db().lock:
            self.assertIsNotNone(self.db().conn.execute(
                "SELECT * FROM cursors WHERE batch_ref='B-001'").fetchone())
            self.assertIsNone(self.db().conn.execute(
                "SELECT * FROM archives WHERE batch_ref='B-001'").fetchone())

    # ---- 6. 已封存批次：拒收回执/禁止重试/不再进入转发队列 ----
    def test_archived_batch_never_reenters_queue(self):
        self.forward_all("B-001", [1, 2])
        self.seal_ok("B-001")
        calls_before = self.h.call_count("B-001:")

        # 新回执（单条与批量）一律拒收
        code, err = self.h.call("POST", "/receipts",
                                {**receipt(3, "R-3", "arrived"),
                                 "batch_ref": "B-001"}, expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "batch_archived")
        code, err = self.h.call("POST", "/receipts/batch",
                                batch_upload("B-001", [receipt(3, "R-3")]),
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "batch_archived")
        _, conflicts = self.h.call("GET", "/conflicts")
        self.assertTrue(any(c["reason"] == "batch_archived"
                            for c in conflicts["conflicts"]))

        # 重试与终态确认一律拒绝
        code, err = self.h.call("POST", "/retries", {"batch_ref": "B-001"},
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "batch_archived")
        code, err = self.h.call(
            "POST", "/batches/B-001/receipts/1/ack-terminal", {},
            expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "batch_archived")

        # 队列无该批次；适配器不再被调用
        _, q = self.h.call("GET", "/queue")
        self.assertFalse(any(r["batch_ref"] == "B-001" for r in q["pending"]))
        time.sleep(0.3)
        self.h.app.forwarder.cycle()
        self.assertEqual(self.h.call_count("B-001:"), calls_before)

    # ---- 7. 下载归档：字节一致、清单可独立复核 ----
    def test_download_archive_byte_identical_and_verifiable(self):
        self.forward_all("B-001", [1, 2])
        res = self.seal_ok("B-001")

        def download():
            req = urllib.request.Request(
                f"{self.h.base}/archives/B-001/download")
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, resp.headers, resp.read()

        status, headers, raw1 = download()
        self.assertEqual(status, 200)
        self.assertIn("attachment", headers.get("Content-Disposition", ""))
        self.assertEqual(headers.get("X-Manifest-Hash"), res["manifest_hash"])
        _, _, raw2 = download()
        self.assertEqual(raw1, raw2)  # 重复下载字节一致

        data = json.loads(raw1.decode("utf-8"))
        self.assertEqual(data["format"], "relay-archive/v1")
        # 清单哈希独立复核
        self.assertEqual(
            core.sha256_hex(core.canonical(data["manifest"])),
            data["manifest_hash"])
        # 执行包与逐条回执哈希独立复核
        self.assertEqual(
            core.sha256_hex(core.canonical(data["package"])),
            data["manifest"]["package"]["content_hash"])
        self.assertEqual(len(data["receipts"]), 2)
        by_seq = {e["seq"]: e for e in data["manifest"]["receipts"]}
        for rec in data["receipts"]:
            self.assertEqual(
                core.sha256_hex(core.canonical(rec["payload"])),
                by_seq[rec["seq"]]["content_hash"])
        # 审计时间线快照随归档保存
        self.assertEqual(len(data["audit"]),
                         data["manifest"]["audit_count"])
        self.assertTrue(any(e["kind"] == "receipt_received"
                            for e in data["audit"]))

    # ---- 8. 校验发现篡改并拒绝恢复 ----
    def test_verify_detects_tamper_and_restore_rejected(self):
        self.forward_all("B-001", [1])
        self.seal_ok("B-001")
        _, ok = self.h.call("POST", "/archives/B-001/verify", {})
        self.assertTrue(ok["ok"])
        self.assertTrue(all(ok["checks"].values()))

        # 篡改归档包内回执编号
        with self.db().lock:
            arow = self.db().conn.execute(
                "SELECT payload FROM archives WHERE batch_ref='B-001'").fetchone()
            payload = json.loads(arow["payload"])
            payload["receipts"][0]["receipt_no"] = "R-FORGED"
            self.db().conn.execute(
                "UPDATE archives SET payload=? WHERE batch_ref='B-001'",
                (json.dumps(payload, ensure_ascii=False),))
            self.db().commit()

        code, err = self.h.call("POST", "/archives/B-001/verify", {},
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "checksum_mismatch")
        self.assertFalse(err["checks"]["receipt_hashes"])
        # 校验失败 → 恢复被拒绝
        code, err = self.h.call("POST", "/archives/B-001/restore", {},
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "checksum_mismatch")
        _, meta = self.h.call("GET", "/archives/B-001")
        self.assertIsNone(meta["restored_at"])
        self.assertFalse(meta["last_verify_ok"])

    # ---- 9. 校验并恢复只读详情 ----
    def test_restore_read_only_detail(self):
        self.forward_all("B-001", [1, 2])
        self.seal_ok("B-001")

        # 恢复前：只读详情不可用
        code, err = self.h.call("GET", "/archives/B-001/detail",
                                expect_error=True)
        self.assertEqual(code, 409)
        self.assertEqual(err["code"], "not_restored")
        _, d = self.h.call("GET", "/batches/B-001")
        self.assertEqual(d["receipts"], [])
        self.assertFalse(d["restored"])

        # 校验并恢复
        _, res = self.h.call("POST", "/archives/B-001/restore", {})
        self.assertTrue(res["restored"])
        self.assertFalse(res["replay"])
        detail = res["detail"]
        self.assertTrue(detail["read_only"])
        self.assertTrue(detail["archived"])
        self.assertEqual(len(detail["receipts"]), 2)
        self.assertEqual({r["state"] for r in detail["receipts"]}, {"forwarded"})
        self.assertEqual(detail["cursor"], 3)
        self.assertGreaterEqual(len(detail["audit_snapshot"]), 1)

        # 恢复后：批次详情与归档详情均可只读查看
        _, d2 = self.h.call("GET", "/batches/B-001")
        self.assertTrue(d2["restored"])
        self.assertEqual(len(d2["receipts"]), 2)
        _, d3 = self.h.call("GET", "/archives/B-001/detail")
        self.assertEqual(len(d3["receipts"]), 2)

        # 重复恢复幂等
        _, again = self.h.call("POST", "/archives/B-001/restore", {})
        self.assertTrue(again["replay"])
        self.assertEqual(again["restored_at"], res["restored_at"])

        # 恢复不写回热库：转发队列仍无该批次
        self.assertEqual(self.hot_rows("B-001"), 0)
        time.sleep(0.2)
        self.assertEqual(self.h.call_count("B-001:"), 2)

    # ---- 10a. 重启后归档完整，无半迁移状态 ----
    def test_restart_after_seal_keeps_archive_consistent(self):
        self.forward_all("B-001", [1, 2])
        res = self.seal_ok("B-001")
        db_path, tmp_dir = self.h.db_path, self.h.tmp
        self.h.stop(keep_data=True)
        self.h.tmp = None

        h2 = RelayServerHarness(db_path=db_path)
        h2.tmp = tmp_dir
        try:
            # 启动兜底无需清扫（单事务封存无残留）
            self.assertEqual(
                h2.app.startup["archive_sweep"],
                {"receipts_removed": 0, "cursors_removed": 0})
            _, arch = h2.call("GET", "/archives")
            self.assertEqual(len(arch["archives"]), 1)
            self.assertEqual(arch["archives"][0]["manifest_hash"],
                             res["manifest_hash"])
            _, d = h2.call("GET", "/batches/B-001")
            self.assertTrue(d["archived"])
            # 重启后归档记录仍不进入转发队列
            h2.configure({"*": "ack"})
            time.sleep(0.3)
            self.assertEqual(h2.call_count("B-001:"), 0)
            # 校验与恢复在重启后仍可用
            _, ok = h2.call("POST", "/archives/B-001/verify", {})
            self.assertTrue(ok["ok"])
        finally:
            h2.stop()

    # ---- 10b. 启动兜底清扫伪造的半迁移残留 ----
    def test_startup_sweeps_half_migrated_residue(self):
        self.forward_all("B-001", [1])
        self.seal_ok("B-001")
        db_path, tmp_dir = self.h.db_path, self.h.tmp
        self.h.stop(keep_data=True)
        self.h.tmp = None

        # 伪造"封存中途失败"的残留：已封存批次又出现热库回执与游标
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO receipts(batch_ref,seq,receipt_no,receipt_type,"
            "content_hash,payload,state,created_at,updated_at)"
            " VALUES('B-001',2,'R-GHOST','arrived','x','{}','queued',?,?)",
            (time.time(), time.time()))
        conn.execute("INSERT INTO cursors(batch_ref,next_seq) VALUES('B-001',2)")
        conn.commit()
        conn.close()

        h2 = RelayServerHarness(db_path=db_path)
        h2.tmp = tmp_dir
        try:
            self.assertEqual(
                h2.app.startup["archive_sweep"],
                {"receipts_removed": 1, "cursors_removed": 1})
            h2.configure({"*": "ack"})
            time.sleep(0.3)  # 残留行已被清扫，永远不会被派发
            self.assertEqual(h2.call_count("B-001:"), 0)
            with h2.app.db.lock:
                self.assertEqual(h2.app.db.conn.execute(
                    "SELECT COUNT(*) c FROM receipts WHERE batch_ref='B-001'"
                ).fetchone()["c"], 0)
            _, aud = h2.call("GET", "/audit")
            self.assertTrue(any(e["kind"] == "archive_sweep"
                                for e in aud["events"]))
        finally:
            h2.stop()

    # ---- 11. 原审计时间线保留 ----
    def test_audit_timeline_preserved_after_seal(self):
        self.forward_all("B-001", [1, 2])
        self.seal_ok("B-001")
        _, aud = self.h.call("GET", "/audit")
        kinds = {e["kind"] for e in aud["events"] if e["batch_ref"] == "B-001"}
        for need in ("receipt_received", "dispatch", "forward_ok",
                     "batch_sealed"):
            self.assertIn(need, kinds)
        # 归档包内另有审计快照（供离线下载后查看）
        _, meta = self.h.call("GET", "/archives/B-001")
        self.assertGreater(meta["manifest"]["audit_count"], 0)
        self.assertEqual(meta["manifest"]["cursor_final"], 3)
        self.assertEqual(meta["manifest"]["receipt_count"], 2)

    # ---- 12. 封存后预估反映已回收 ----
    def test_estimate_reflects_archived_batches(self):
        self.forward_all("B-001", [1, 2])
        res = self.seal_ok("B-001")
        _, est = self.h.call("GET", "/archives/estimate")
        entry = next(b for b in est["batches"] if b["batch_ref"] == "B-001")
        self.assertTrue(entry["archived"])
        self.assertEqual(entry["bytes_reclaimed"], res["bytes_reclaimed"])
        self.assertEqual(est["total_reclaimable_bytes"], 0)
        self.assertEqual(est["hot_receipt_bytes"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
