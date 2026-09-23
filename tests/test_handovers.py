"""位置交接模块端到端测试:

- 多样品单事务交接:全部样品处于原位置才统一更新
- 任一不符(样品不存在 / 当前位置与声明不一致 / 原地交接)整批不变,
  且一次返回全部逐项冲突
- 交接人、时间、变更前后位置写入不可变明细,触发器禁止改/删
- 交接单号幂等重放;同号不同内容 409
- 按样品查询完整交接历史;并发交接串行化,后到者位置校验失败
"""
from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app import handovers as ho
from app.db import connect, init_db
from app.main import app, get_conn


@pytest.fixture()
def client():
    mem = connect(":memory:")
    init_db(mem)

    def _override():
        yield mem

    app.dependency_overrides[get_conn] = _override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
    mem.close()


def _sample(client, op_no, sid, amount=100, unit="mL", location=None):
    body = {
        "type": "sampling", "op_no": op_no, "sample_id": sid,
        "material": "血清", "amount": amount, "unit": unit,
    }
    if location is not None:
        body["location"] = location
    r = client.post("/api/v1/operations/sampling", json=body)
    assert r.status_code == 200, r.text


def _location(conn, sid):
    return conn.execute(
        "SELECT location FROM samples WHERE sample_id = ?", (sid,)
    ).fetchone()["location"]


# ---------------------------------------------------------------- 提交 / 查询

def test_handover_multiple_samples_all_moved_together(client):
    _sample(client, "OP-1", "S1", location="冷库A-1")
    _sample(client, "OP-2", "S2", location="冷库A-1")

    r = client.post("/api/v1/handovers", json={
        "handover_no": "HO-1",
        "operator": "李交接",
        "note": "冷链转运",
        "items": [
            {"sample_id": "S1", "from_location": "冷库A-1", "to_location": "冷库B-2"},
            {"sample_id": "S2", "from_location": "冷库A-1", "to_location": "冷库B-2"},
        ],
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["handover_no"] == "HO-1"
    assert body["operator"] == "李交接"
    assert body["note"] == "冷链转运"
    assert body["replayed"] is False
    assert body["created_at"]
    assert [(i["sample_id"], i["from_location"], i["to_location"]) for i in body["items"]] == [
        ("S1", "冷库A-1", "冷库B-2"),
        ("S2", "冷库A-1", "冷库B-2"),
    ]

    # 样品位置已统一更新
    r = client.get("/api/v1/samples")
    locs = {x["sample_id"]: x["location"] for x in r.json()["items"]}
    assert locs == {"S1": "冷库B-2", "S2": "冷库B-2"}

    # 按交接单号查询
    r = client.get("/api/v1/handovers/HO-1")
    assert r.status_code == 200
    assert r.json()["operator"] == "李交接"
    assert len(r.json()["items"]) == 2


def test_handover_one_location_mismatch_rolls_back_whole_batch(client):
    _sample(client, "OP-1", "S1", location="冷库A-1")
    _sample(client, "OP-2", "S2", location="冷库C-9")  # 已不在 A-1

    r = client.post("/api/v1/handovers", json={
        "handover_no": "HO-BAD",
        "operator": "李交接",
        "items": [
            {"sample_id": "S1", "from_location": "冷库A-1", "to_location": "冷库B-2"},
            {"sample_id": "S2", "from_location": "冷库A-1", "to_location": "冷库B-2"},
        ],
    })
    assert r.status_code == 409
    conflicts = r.json()["conflicts"]
    assert len(conflicts) == 1
    c = conflicts[0]
    assert c["code"] == "location_mismatch"
    assert c["sample_id"] == "S2"
    assert c["op_no"] == "HO-BAD"
    assert c["location"] == "冷库A-1"          # 请求声明的原位置
    assert c["current_location"] == "冷库C-9"  # 数据库实际位置

    # 整批不变:连位置正确的 S1 也不能动
    r = client.get("/api/v1/samples")
    locs = {x["sample_id"]: x["location"] for x in r.json()["items"]}
    assert locs == {"S1": "冷库A-1", "S2": "冷库C-9"}
    # 交接单与明细均未落库
    r = client.get("/api/v1/handovers/HO-BAD")
    assert r.status_code == 404


def test_handover_collects_all_item_conflicts_at_once(client):
    _sample(client, "OP-1", "S1", location="冷库A-1")
    _sample(client, "OP-3", "S3", location="冷库C-9")
    # S2 不存在;S3 位置不符;S1 原位置=目标位置
    r = client.post("/api/v1/handovers", json={
        "handover_no": "HO-BAD2",
        "operator": "李交接",
        "items": [
            {"sample_id": "S2", "from_location": "冷库A-1", "to_location": "冷库B-2"},
            {"sample_id": "S3", "from_location": "冷库X", "to_location": "冷库B-2"},
            {"sample_id": "S1", "from_location": "冷库A-1", "to_location": "冷库A-1"},
        ],
    })
    assert r.status_code == 409
    conflicts = r.json()["conflicts"]
    by_sample = {c["sample_id"]: c["code"] for c in conflicts}
    assert by_sample == {
        "S2": "sample_not_found",
        "S3": "location_mismatch",
        "S1": "same_location",
    }
    s3 = next(c for c in conflicts if c["sample_id"] == "S3")
    assert s3["current_location"] == "冷库C-9"


def test_handover_from_null_location_requires_null_current(client):
    # 取样时未给位置 -> 当前位置为 null,允许以 null 原位置交接
    _sample(client, "OP-1", "S1")
    r = client.post("/api/v1/handovers", json={
        "handover_no": "HO-N1",
        "operator": "李交接",
        "items": [
            {"sample_id": "S1", "from_location": None, "to_location": "冷库B-2"}
        ],
    })
    assert r.status_code == 200, r.text
    assert r.json()["items"][0]["from_location"] is None

    # 样品已有位置,却声明原位置为 null -> 冲突
    r = client.post("/api/v1/handovers", json={
        "handover_no": "HO-N2",
        "operator": "李交接",
        "items": [
            {"sample_id": "S1", "from_location": None, "to_location": "冷库D-1"}
        ],
    })
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "location_mismatch"
    assert c["location"] is None
    assert c["current_location"] == "冷库B-2"


def test_handover_duplicate_sample_id_rejected_by_schema(client):
    _sample(client, "OP-1", "S1", location="A")
    r = client.post("/api/v1/handovers", json={
        "handover_no": "HO-DUP",
        "operator": "李交接",
        "items": [
            {"sample_id": "S1", "from_location": "A", "to_location": "B"},
            {"sample_id": "S1", "from_location": "A", "to_location": "C"},
        ],
    })
    assert r.status_code == 422


# ---------------------------------------------------------------- 幂等

def test_handover_idempotent_replay_and_payload_conflict(client):
    _sample(client, "OP-1", "S1", location="冷库A-1")
    payload = {
        "handover_no": "HO-IDEM",
        "operator": "李交接",
        "items": [
            {"sample_id": "S1", "from_location": "冷库A-1", "to_location": "冷库B-2"}
        ],
    }
    r1 = client.post("/api/v1/handovers", json=payload)
    assert r1.status_code == 200
    assert r1.json()["replayed"] is False

    # 相同请求体重放:返回首次结果,位置不再变化
    r2 = client.post("/api/v1/handovers", json=payload)
    assert r2.status_code == 200
    body = r2.json()
    assert body["replayed"] is True
    assert body["created_at"] == r1.json()["created_at"]
    assert body["items"][0]["from_location"] == "冷库A-1"
    assert body["items"][0]["to_location"] == "冷库B-2"

    # 同号不同内容 -> 409(此时 S1 已在 B-2,新请求即便合法也因幂等键冲突被拒)
    r3 = client.post("/api/v1/handovers", json={
        "handover_no": "HO-IDEM",
        "operator": "王交接",
        "items": [
            {"sample_id": "S1", "from_location": "冷库B-2", "to_location": "冷库D-1"}
        ],
    })
    assert r3.status_code == 409
    assert r3.json()["conflicts"][0]["code"] == "handover_no_conflict"


# ---------------------------------------------------------------- 历史

def test_handover_history_per_sample_in_time_order(client):
    _sample(client, "OP-1", "S1", location="冷库A-1")
    _sample(client, "OP-2", "S2", location="冷库A-1")

    client.post("/api/v1/handovers", json={
        "handover_no": "HO-H1", "operator": "李交接",
        "items": [
            {"sample_id": "S1", "from_location": "冷库A-1", "to_location": "操作台1"},
            {"sample_id": "S2", "from_location": "冷库A-1", "to_location": "操作台1"},
        ],
    })
    client.post("/api/v1/handovers", json={
        "handover_no": "HO-H2", "operator": "王交接", "note": "实验后归位",
        "items": [
            {"sample_id": "S1", "from_location": "操作台1", "to_location": "冷库A-2"},
        ],
    })

    r = client.get("/api/v1/samples/S1/handovers")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["sample_id"] == "S1"
    assert body["current_location"] == "冷库A-2"
    assert body["total"] == 2
    assert [(i["handover_no"], i["operator"], i["from_location"], i["to_location"])
            for i in body["items"]] == [
        ("HO-H1", "李交接", "冷库A-1", "操作台1"),
        ("HO-H2", "王交接", "操作台1", "冷库A-2"),
    ]
    assert body["items"][0]["created_at"]
    assert body["items"][1]["note"] == "实验后归位"

    # S2 只参与过一次
    r = client.get("/api/v1/samples/S2/handovers")
    body2 = r.json()
    assert body2["total"] == 1
    assert body2["current_location"] == "操作台1"
    assert body2["items"][0]["to_location"] == "操作台1"

    # 从未交接过的样品:空历史
    _sample(client, "OP-3", "S3", location="冷库E-1")
    r = client.get("/api/v1/samples/S3/handovers")
    assert r.status_code == 200
    assert r.json() == {
        "sample_id": "S3", "current_location": "冷库E-1",
        "total": 0, "items": [],
    }

    # 不存在的样品 -> 409 sample_not_found
    r = client.get("/api/v1/samples/NOPE/handovers")
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "sample_not_found"


def test_handover_records_are_immutable(tmp_path):
    from app.schemas import HandoverRequest

    db_path = str(tmp_path / "i.db")
    conn = connect(db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, location, is_root)"
        " VALUES ('S1', 'M', 'mL', 100, 100, 'A', 1)"
    )
    conn.commit()
    ho.create_handover(conn, HandoverRequest(
        handover_no="HO-IMM", operator="李交接",
        items=[{"sample_id": "S1", "from_location": "A",
                "to_location": "B"}],  # type: ignore[arg-type]
    ))

    with pytest.raises(Exception, match="不可变"):
        conn.execute(
            "UPDATE handover_items SET to_location = 'X'"
            " WHERE handover_no = 'HO-IMM'"
        )
    with pytest.raises(Exception, match="不可变"):
        conn.execute(
            "DELETE FROM handover_items WHERE handover_no = 'HO-IMM'"
        )
    conn.close()


# ---------------------------------------------------------------- 并发

def test_concurrent_handovers_serialize_stale_from_location_loses(tmp_path):
    """两个线程并发把同一样品从 A 交接到不同目标:先到者成功,
    后到者因样品已不在声明原位置而整批失败,位置不被覆盖。"""
    db_path = str(tmp_path / "c.db")
    setup = connect(db_path)
    init_db(setup)
    setup.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, location, is_root)"
        " VALUES ('S1', 'M', 'mL', 100, 100, 'A', 1)"
    )
    setup.commit()
    setup.close()

    from app.schemas import HandoverRequest

    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def worker(no: str, to_loc: str) -> None:
        c = connect(db_path)
        barrier.wait()
        try:
            ho.create_handover(c, HandoverRequest(
                handover_no=no, operator="李交接",
                items=[{"sample_id": "S1", "from_location": "A",
                        "to_location": to_loc}],  # type: ignore[arg-type]
            ))
            outcomes.append("ok")
        except ho.HandoverError:
            outcomes.append("conflict")
        finally:
            c.close()

    t1 = threading.Thread(target=worker, args=("HC-1", "B"))
    t2 = threading.Thread(target=worker, args=("HC-2", "C"))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert sorted(outcomes) == ["conflict", "ok"]
    check = connect(db_path)
    assert _location(check, "S1") == "B"  # 只有先提交者生效
    n = check.execute(
        "SELECT COUNT(*) AS c FROM handover_items WHERE sample_id = 'S1'"
    ).fetchone()["c"]
    assert n == 1
    check.close()
