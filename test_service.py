"""Реальные HTTP-тесты запущенного сервиса на учебной ВМ."""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
import uuid
from contextlib import closing
from pathlib import Path

import pyarrow.parquet as pq

BASE = os.getenv("SERVICE_URL", "http://127.0.0.1:8000").rstrip("/")
EXPECTED_INSTANCE = os.getenv("RECSYS_INSTANCE_ID", "")
OFFLINE_PATH = Path(os.environ["RECSYS_OFFLINE"])
SIMILAR_PATH = Path(os.environ["RECSYS_SIMILAR"])
DB_PATH = Path(os.environ["RECSYS_HISTORY_DB"])


def request(path: str, method: str = "GET", payload: dict | None = None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        BASE + path, method=method, data=data,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return res.status, json.loads(res.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read().decode("utf-8"))


def personalized_fixture() -> tuple[int, list[int]]:
    """Найдём пользователя с полными десятью рекомендациями в реальном Parquet."""
    pf = pq.ParquetFile(OFFLINE_PATH)
    groups: dict[int, dict[int, int]] = {}
    for batch in pf.iter_batches(columns=["user_id", "track_id", "rank"], batch_size=10_000):
        users, tracks, ranks = (col.to_pylist() for col in batch.columns)
        for u, t, rank in zip(users, tracks, ranks):
            bucket = groups.setdefault(int(u), {})
            bucket[int(rank)] = int(t)
            if len(bucket) == 10 and all(k in bucket for k in range(1, 11)):
                values = [bucket[k] for k in range(1, 11)]
                if len(set(values)) == 10:
                    return int(u), values
        if len(groups) > 1000:
            raise RuntimeError("Не найден пользователь с десятью рекомендациями")
    raise RuntimeError("Офлайн-рекомендации не содержат полного списка")


def online_fixture(base: list[int]) -> tuple[int, int]:
    """Найдём трек с i2i-соседом, которого нет в выбранной офлайн-выдаче."""
    base_set = set(base)
    pf = pq.ParquetFile(SIMILAR_PATH)
    for batch in pf.iter_batches(columns=["track_id", "similar_track_id", "rank"], batch_size=10_000):
        sources, destinations, ranks = (col.to_pylist() for col in batch.columns)
        for src, dst, rank in zip(sources, destinations, ranks):
            src, dst = int(src), int(dst)
            if int(rank) == 1 and src not in base_set and dst not in base_set and src != dst:
                return src, dst
    raise RuntimeError("Не найден i2i-кандидат, подходящий для независимого онлайн-теста")


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    status, health = request("/health")
    check(status == 200 and health.get("status") == "ok", "Сервис не отвечает на /health")
    check(bool(EXPECTED_INSTANCE) and health.get("instance_id") == EXPECTED_INSTANCE,
          "Отвечает не тот экземпляр сервиса")
    print(f"HEALTH_OK instance={EXPECTED_INSTANCE}", flush=True)

    user, expected = personalized_fixture()
    seed, online_candidate = online_fixture(expected)
    cold = -987654321

    status, a = request(f"/recommendations/{cold}")
    check(status == 200, f"Сценарий A: HTTP {status}")
    check(a["source"] == "popular" and len(a["recommendations"]) == 10,
          "Сценарий A: нет десяти популярных треков")
    check(len(set(a["recommendations"])) == 10, "Сценарий A: повторения")
    check(a["online_history_size"] == 0, "Сценарий A: неожиданная история")
    print("PASS A: unknown user -> 10 distinct popular recommendations", flush=True)

    status, b = request(f"/recommendations/{user}")
    check(status == 200, f"Сценарий B: HTTP {status}")
    check(b["source"] == "personal" and b["online_history_size"] == 0,
          "Сценарий B: нет персональной выдачи или база не изолирована")
    check(b["recommendations"] == expected, "Сценарий B: нарушен порядок офлайн-рангов")
    print(f"PASS B: user_id={user}, offline ranks match Parquet", flush=True)

    event_id = "integration-" + uuid.uuid4().hex
    try:
        status, event = request("/events", "POST", {
            "user_id": user, "track_id": seed, "event_id": event_id})
        check(status == 201 and event.get("stored") is True,
              "Сценарий C: событие не сохранено")
        status, c = request(f"/recommendations/{user}")
        check(status == 200, f"Сценарий C: HTTP {status}")
        result = c["recommendations"]
        check(c["online_history_size"] == 1 and c["online_recommendations"] >= 1,
              "Сценарий C: онлайн-история не учтена")
        check(online_candidate in result and online_candidate not in expected,
              "Сценарий C: новый i2i-кандидат не попал в выдачу")
        check(result != b["recommendations"], "Сценарий C: выдача не изменилась")
        check(seed not in result, "Сценарий C: прослушанный трек присутствует в ответе")
        check(len(result) == 10 and len(set(result)) == 10,
              "Сценарий C: неверный размер или повторения")
        print(f"PASS C: online history affects recommendations (new track_id={online_candidate})",
              flush=True)
    finally:
        # Удаляем только созданное этим тестом событие в отдельной тестовой БД.
        with closing(sqlite3.connect(DB_PATH)) as db, db:
            db.execute("DELETE FROM events WHERE event_id=?", (event_id,))

    status, _ = request("/events", "POST", {
        "user_id": user, "track_id": -987654321, "event_id": uuid.uuid4().hex})
    check(status == 404, "Неизвестный трек должен возвращать HTTP 404")
    print("PASS D: unknown track -> HTTP 404", flush=True)
    print("ALL HTTP INTEGRATION SCENARIOS PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
