"""位置交接服务:一次提交多个样品的原位置与目标位置。

- 交接:在单个 `BEGIN IMMEDIATE` 事务内逐项校验每个样品仍处于请求所述的
  原位置(样品存在 + 当前位置与 from_location 一致);任一不符即整批回滚,
  任何样品的位置都不变。全部通过后统一更新各样品到目标位置,并逐项写入
  不可变交接明细(变更前后位置)与交接单(交接人、交接时间)。
- 幂等:同一交接单号 + 同一请求体重放返回首次结果;同号不同内容 409。
- 历史:按样品查询其全部位置交接记录(交接人、时间、变更前后位置)。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from . import schemas as s


class HandoverError(Exception):
    """位置交接业务冲突,携带结构化冲突列表。"""

    def __init__(self, conflicts: list[s.Conflict]):
        self.conflicts = conflicts
        super().__init__(conflicts[0].message if conflicts else "handover conflict")


def _conflict(
    code: str,
    message: str,
    *,
    handover_no: Optional[str] = None,
    sample_id: Optional[str] = None,
    location: Optional[str] = None,
    current_location: Optional[str] = None,
) -> s.Conflict:
    return s.Conflict(
        code=code,
        message=message,
        op_no=handover_no,
        sample_id=sample_id,
        location=location,
        current_location=current_location,
    )


# ---------------------------------------------------------------- 结果组装

def _load_items(
    conn: sqlite3.Connection, handover_no: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM handover_items WHERE handover_no = ?"
        " ORDER BY seq_index, item_id",
        (handover_no,),
    ).fetchall()


def _build_result(
    conn: sqlite3.Connection,
    header: sqlite3.Row,
    *,
    replayed: bool,
) -> s.HandoverResult:
    return s.HandoverResult(
        handover_no=header["handover_no"],
        operator=header["operator"],
        note=header["note"],
        replayed=replayed,
        created_at=header["created_at"],
        items=[
            s.HandoverItemOut(
                sample_id=r["sample_id"],
                from_location=r["from_location"],
                to_location=r["to_location"],
            )
            for r in _load_items(conn, header["handover_no"])
        ],
    )


# ---------------------------------------------------------------- 提交交接

def create_handover(
    conn: sqlite3.Connection, req: s.HandoverRequest
) -> s.HandoverResult:
    """同一事务内校验全部样品仍处于请求所述原位置,任一不符整批不变。"""
    payload = req.model_dump_json()

    # 批内样品号去重(model 已保证,防御性处理)
    sample_ids = list(dict.fromkeys(i.sample_id for i in req.items))

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT * FROM handovers WHERE handover_no = ?",
            (req.handover_no,),
        ).fetchone()
        if existing is not None:
            if existing["payload"] != payload:
                raise HandoverError([
                    _conflict(
                        "handover_no_conflict",
                        f"交接单号 {req.handover_no} 已存在但请求内容不同,"
                        "同一交接单号必须对应同一次交接(幂等键冲突)",
                        handover_no=req.handover_no,
                    )
                ])
            result = _build_result(conn, existing, replayed=True)
            conn.execute("COMMIT")
            return result

        qmarks = ",".join("?" * len(sample_ids))
        rows = {
            r["sample_id"]: r
            for r in conn.execute(
                f"SELECT sample_id, location FROM samples"
                f" WHERE sample_id IN ({qmarks})",
                sample_ids,
            ).fetchall()
        }

        # 先做全部静态校验(样品存在、原位置非同位置、当前位置与声明一致),
        # 收集逐项冲突后一次性抛出,保证"任一不符则整批不变"
        conflicts: list[s.Conflict] = []
        for idx, item in enumerate(req.items):
            row = rows.get(item.sample_id)
            if row is None:
                conflicts.append(_conflict(
                    "sample_not_found",
                    f"样品 {item.sample_id} 不存在,无法交接位置",
                    handover_no=req.handover_no,
                    sample_id=item.sample_id,
                ))
                continue
            current = row["location"]
            if item.from_location == item.to_location:
                conflicts.append(_conflict(
                    "same_location",
                    f"样品 {item.sample_id} 的原位置与目标位置相同"
                    f"({item.to_location}),无需交接",
                    handover_no=req.handover_no,
                    sample_id=item.sample_id,
                    location=item.from_location,
                ))
                continue
            # 样品必须仍处于请求所述原位置(含 null 与 null 的情形)
            if (current is None) != (item.from_location is None) \
                    or current != item.from_location:
                conflicts.append(_conflict(
                    "location_mismatch",
                    f"样品 {item.sample_id} 当前不在所述原位置:"
                    f"请求声明原位置 {item.from_location!r},"
                    f"数据库当前位置 {current!r},整批拒绝交接",
                    handover_no=req.handover_no,
                    sample_id=item.sample_id,
                    location=item.from_location,
                    current_location=current,
                ))
        if conflicts:
            raise HandoverError(conflicts)

        # 全部通过:统一更新位置并写交接单与不可变明细
        conn.execute(
            "INSERT INTO handovers (handover_no, operator, payload, note)"
            " VALUES (?, ?, ?, ?)",
            (req.handover_no, req.operator, payload, req.note),
        )
        for idx, item in enumerate(req.items):
            conn.execute(
                "UPDATE samples SET location = ? WHERE sample_id = ?",
                (item.to_location, item.sample_id),
            )
            conn.execute(
                "INSERT INTO handover_items (handover_no, sample_id,"
                " from_location, to_location, seq_index)"
                " VALUES (?, ?, ?, ?, ?)",
                (req.handover_no, item.sample_id,
                 item.from_location, item.to_location, idx),
            )

        header = conn.execute(
            "SELECT * FROM handovers WHERE handover_no = ?",
            (req.handover_no,),
        ).fetchone()
        result = _build_result(conn, header, replayed=False)  # type: ignore[arg-type]
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------- 查询

def get_handover(
    conn: sqlite3.Connection, handover_no: str
) -> s.HandoverResult:
    header = conn.execute(
        "SELECT * FROM handovers WHERE handover_no = ?",
        (handover_no,),
    ).fetchone()
    if header is None:
        raise HandoverError([
            _conflict(
                "handover_not_found",
                f"交接单号 {handover_no} 不存在",
                handover_no=handover_no,
            )
        ])
    return _build_result(conn, header, replayed=False)


def get_sample_history(
    conn: sqlite3.Connection, sample_id: str
) -> s.HandoverHistoryResponse:
    """按样品查询交接历史(交接人、时间、变更前后位置),按时间先后排列。"""
    sample = conn.execute(
        "SELECT location FROM samples WHERE sample_id = ?",
        (sample_id,),
    ).fetchone()
    if sample is None:
        raise HandoverError([
            _conflict(
                "sample_not_found",
                f"样品 {sample_id} 不存在",
                sample_id=sample_id,
            )
        ])

    rows = conn.execute(
        """SELECT hi.handover_no AS handover_no, h.operator AS operator,
                  h.note AS note, hi.sample_id AS sample_id,
                  hi.from_location AS from_location,
                  hi.to_location AS to_location,
                  hi.seq_index AS seq_index, h.created_at AS created_at
             FROM handover_items hi
             JOIN handovers h ON h.handover_no = hi.handover_no
            WHERE hi.sample_id = ?
            ORDER BY h.created_at, hi.item_id""",
        (sample_id,),
    ).fetchall()
    return s.HandoverHistoryResponse(
        sample_id=sample_id,
        current_location=sample["location"],
        total=len(rows),
        items=[
            s.HandoverHistoryItem(
                handover_no=r["handover_no"],
                operator=r["operator"],
                note=r["note"],
                sample_id=r["sample_id"],
                from_location=r["from_location"],
                to_location=r["to_location"],
                seq_index=r["seq_index"],
                created_at=r["created_at"],
            )
            for r in rows
        ],
    )
