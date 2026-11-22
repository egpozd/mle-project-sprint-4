"""Сервис рекомендаций: готовая выдача, похожие треки и недавняя история."""
from __future__ import annotations

import os
import sqlite3
from array import array
from collections import Counter, OrderedDict
from contextlib import asynccontextmanager, closing
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

K = 10
MAX_HISTORY = 20
OFFLINE_PATH = Path(os.getenv("RECSYS_OFFLINE", "/home/ubuntu/recsys-data/output/recommendations.parquet"))
POPULAR_PATH = Path(os.getenv("RECSYS_POPULAR", "/home/ubuntu/recsys-data/output/top_popular.parquet"))
SIMILAR_PATH = Path(os.getenv("RECSYS_SIMILAR", "/home/ubuntu/recsys-data/output/similar.parquet"))
ITEMS_PATH = os.getenv("RECSYS_ITEMS", "")
DB_PATH = Path(os.getenv("RECSYS_HISTORY_DB", "/home/ubuntu/recsys-data/service-stage4/history.sqlite3"))
INSTANCE_ID = os.getenv("RECSYS_INSTANCE_ID", "local")


class Event(BaseModel):
    user_id: int
    track_id: int
    event_id: str = Field(min_length=1, max_length=128)


class RecommendationStore:
    """В памяти держим только ID и позиции, а не полные таблицы Parquet."""

    def __init__(self) -> None:
        self.offline: dict[int, array] = {}
        self.similar: dict[int, array] = {}
        self.popular: list[int] = []
        self.tracks: set[int] = set()
        self._load_ranked(OFFLINE_PATH, "user_id", "track_id", self.offline)
        self._load_popular(POPULAR_PATH)
        self._load_ranked(SIMILAR_PATH, "track_id", "similar_track_id", self.similar)
        if ITEMS_PATH:
            self._load_items(Path(ITEMS_PATH))
        if not self.offline or not self.popular or not self.similar:
            raise RuntimeError("Один из источников рекомендаций пуст")

    @staticmethod
    def _file(path: Path, cols: set[str]) -> pq.ParquetFile:
        if not path.is_file():
            raise RuntimeError(f"Не найден Parquet: {path}")
        pf = pq.ParquetFile(path)
        missing = cols - set(pf.schema_arrow.names)
        if missing:
            raise RuntimeError(f"{path}: отсутствуют колонки {sorted(missing)}")
        return pf

    def _load_ranked(self, path: Path, source_col: str, dest_col: str,
                     target: dict[int, array]) -> None:
        pf = self._file(path, {source_col, dest_col, "rank"})
        for batch in pf.iter_batches(columns=[source_col, dest_col, "rank"], batch_size=100_000):
            sources, destinations, ranks = (col.to_pylist() for col in batch.columns)
            for raw_source, raw_dest, raw_rank in zip(sources, destinations, ranks):
                if raw_source is None or raw_dest is None or raw_rank is None:
                    raise RuntimeError(f"Пустые значения в {path}")
                source, dest, rank = int(raw_source), int(raw_dest), int(raw_rank)
                if dest < 0 or not 1 <= rank <= K:
                    raise RuntimeError(f"Некорректный ID или ранг в {path}")
                ranks_for_source = target.get(source)
                if ranks_for_source is None:
                    ranks_for_source = array("q", [-1] * K)
                    target[source] = ranks_for_source
                if ranks_for_source[rank - 1] != -1:
                    raise RuntimeError(f"Повтор ранга {rank} для ID {source} в {path}")
                ranks_for_source[rank - 1] = dest
                self.tracks.add(dest)
                if source_col == "track_id":
                    self.tracks.add(source)

    def _load_popular(self, path: Path) -> None:
        # top_popular.parquet содержит ТОП-10 отдельно для каждого пользователя.
        # Частоты попадания в пользовательские списки дают общий запасной ТОП.
        pf = self._file(path, {"track_id", "rank"})
        counts: Counter[int] = Counter()
        ranks_sum: Counter[int] = Counter()
        for batch in pf.iter_batches(columns=["track_id", "rank"], batch_size=100_000):
            track_ids, ranks = (col.to_pylist() for col in batch.columns)
            for raw_track, raw_rank in zip(track_ids, ranks):
                if raw_track is None or raw_rank is None:
                    continue
                track, rank = int(raw_track), int(raw_rank)
                counts[track] += 1
                ranks_sum[track] += rank
        self.popular = sorted(counts, key=lambda t: (-counts[t], ranks_sum[t] / counts[t], t))[:1000]
        self.tracks.update(counts)

    def _load_items(self, path: Path) -> None:
        pf = self._file(path, {"track_id"})
        for batch in pf.iter_batches(columns=["track_id"], batch_size=100_000):
            self.tracks.update(int(t) for t in batch.column(0).to_pylist() if t is not None)

    @staticmethod
    def _ranked(values: array | None):
        return (t for t in values if t >= 0) if values is not None else iter(())

    def recommendations(self, user: int, history: list[int], limit: int = K) -> tuple[list[int], str, int]:
        seen = set(history)
        online: OrderedDict[int, None] = OrderedDict()
        for track in reversed(history[-MAX_HISTORY:]):
            for candidate in self._ranked(self.similar.get(track)):
                if candidate not in seen:
                    online.setdefault(candidate, None)

        base = self.offline.get(user)
        source = "personal" if base is not None else "popular"
        offline_list = self._ranked(base) if base is not None else iter(self.popular)
        sources = [iter(online), iter(offline_list)]
        output: list[int] = []
        used: set[int] = set()
        online_used = 0

        def next_valid(idx: int) -> int | None:
            for track in sources[idx]:
                if track not in seen and track not in used:
                    return track
            return None

        # Для позиций 1, 4 и 7 сначала используем похожие треки. Если остальных рекомендаций не хватает, похожие треки могут занять и другие позиции.
        for position in range(limit):
            prefer_online = position in (0, 3, 6)
            chosen_source = 0 if prefer_online else 1
            candidate = next_valid(chosen_source)
            if candidate is None:
                chosen_source = 1 - chosen_source
                candidate = next_valid(chosen_source)
            if candidate is None:
                break
            output.append(candidate)
            used.add(candidate)
            online_used += int(chosen_source == 0)

        if len(output) < limit:
            for candidate in self.popular:
                if candidate not in seen and candidate not in used:
                    output.append(candidate)
                    used.add(candidate)
                    if len(output) == limit:
                        break
        return output, source, online_used


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect()) as db, db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE IF NOT EXISTS events ("
                   "event_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, "
                   "track_id INTEGER NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id)")
    app.state.store = RecommendationStore()
    yield


app = FastAPI(title="Music recommendations", lifespan=lifespan)


@app.post("/events", status_code=201)
def add_event(event: Event) -> dict[str, Any]:
    store: RecommendationStore = app.state.store
    if event.track_id not in store.tracks:
        raise HTTPException(404, "Неизвестный track_id")
    with closing(connect()) as db, db:
        cursor = db.execute("INSERT OR IGNORE INTO events(event_id,user_id,track_id) VALUES (?,?,?)",
                            (event.event_id, event.user_id, event.track_id))
        added = cursor.rowcount == 1
    return {"stored": added, "user_id": event.user_id, "track_id": event.track_id}


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: int, limit: int = K) -> dict[str, Any]:
    if not 1 <= limit <= K:
        raise HTTPException(422, f"limit должен быть от 1 до {K}")
    store: RecommendationStore = app.state.store
    with closing(connect()) as db:
        history = [row[0] for row in db.execute(
            "SELECT track_id FROM events WHERE user_id=? ORDER BY rowid", (user_id,))]
    recs, source, online_count = store.recommendations(user_id, history, limit)
    return {"user_id": user_id, "recommendations": recs, "source": source,
            "online_history_size": len(history), "online_recommendations": online_count}


@app.get("/health")
def health() -> dict[str, Any]:
    store: RecommendationStore = app.state.store
    return {"status": "ok", "instance_id": INSTANCE_ID,
            "offline_users": len(store.offline), "known_tracks": len(store.tracks)}
