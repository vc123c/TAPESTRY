from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

import httpx
import polars as pl

from db.connection import ROOT, write_connection
from scrapers.base import BaseScraper


SOURCES = [
    ("Communist Manifesto", "Karl Marx and Friedrich Engels", "socialist", "https://www.gutenberg.org/files/61/61-0.txt"),
    ("Wage Labour and Capital", "Karl Marx", "socialist", "https://www.gutenberg.org/files/35539/35539-0.txt"),
    ("The Jungle", "Upton Sinclair", "progressive", "https://www.gutenberg.org/files/140/140-0.txt"),
    ("On Liberty", "John Stuart Mill", "classical_liberal", "https://www.gutenberg.org/files/34901/34901-0.txt"),
    ("Wealth of Nations Vol. 1", "Adam Smith", "classical_liberal", "https://www.gutenberg.org/files/3300/3300-0.txt"),
    ("Common Sense", "Thomas Paine", "constitutional", "https://www.gutenberg.org/files/147/147-0.txt"),
    ("Rights of Man", "Thomas Paine", "constitutional", "https://www.gutenberg.org/files/1342/1342-0.txt"),
    ("The Federalist Papers", "Hamilton, Madison, and Jay", "constitutionalist", "https://www.gutenberg.org/files/1404/1404-0.txt"),
    ("Anti-Federalist Papers", "Anti-Federalist authors", "constitutionalist", "https://www.gutenberg.org/cache/epub/16960/pg16960.txt"),
    ("Democracy in America Vol. 1", "Alexis de Tocqueville", "communitarian", "https://www.gutenberg.org/files/815/815-0.txt"),
    ("Twenty Years at Hull-House", "Jane Addams", "progressive", "https://www.gutenberg.org/files/1325/1325-0.txt"),
    ("The Souls of Black Folk", "W. E. B. Du Bois", "progressive", "https://www.gutenberg.org/files/408/408-0.txt"),
]

THEMES = {
    "labor": ["labor", "worker", "wage", "union", "factory"],
    "capital": ["capital", "property", "profit", "rent", "market"],
    "freedom": ["freedom", "liberty", "free"],
    "equality": ["equality", "equal", "justice"],
    "government": ["government", "state", "law", "legislature"],
    "rights": ["rights", "right", "constitution"],
    "democracy": ["democracy", "republic", "vote", "majority"],
    "community": ["community", "association", "society"],
    "individual": ["individual", "person", "self"],
    "war": ["war", "army", "military"],
    "taxation": ["tax", "taxation", "revenue"],
    "immigration": ["immigrant", "immigration", "foreigner"],
    "religion": ["religion", "church", "faith"],
    "race": ["race", "negro", "slavery"],
    "class": ["class", "bourgeois", "proletarian"],
}


class IdeologyCorpusScraper(BaseScraper):
    source_name = "ideology_corpus"
    output_path = "data/raw/ideology_corpus_latest.parquet"

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def _ensure_schema(self) -> None:
        with write_connection() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS ideology_corpus_chunks (
                    chunk_id VARCHAR PRIMARY KEY,
                    source_title VARCHAR,
                    source_text VARCHAR,
                    author VARCHAR,
                    ideology_frame VARCHAR,
                    publication_year INTEGER,
                    source_url VARCHAR,
                    chunk_index INTEGER,
                    text TEXT,
                    chunk_text TEXT,
                    ideology_tags VARCHAR[],
                    key_themes VARCHAR[],
                    embedding BLOB,
                    word_count INTEGER,
                    created_at TIMESTAMP,
                    fetched_at TIMESTAMP
                )
                """
            )
            for col, ddl in {
                "source_text": "VARCHAR",
                "ideology_frame": "VARCHAR",
                "chunk_text": "TEXT",
                "key_themes": "VARCHAR[]",
                "embedding": "BLOB",
                "word_count": "INTEGER",
                "created_at": "TIMESTAMP",
            }.items():
                con.execute(f"ALTER TABLE ideology_corpus_chunks ADD COLUMN IF NOT EXISTS {col} {ddl}")

    @staticmethod
    def _slug(title: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")

    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"\*\*\* START OF.*?\*\*\*", "", text, flags=re.I | re.S)
        text = re.sub(r"\*\*\* END OF.*", "", text, flags=re.I | re.S)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _chunks(text: str, size: int = 500, overlap: int = 50) -> list[str]:
        words = text.split()
        chunks = []
        step = max(1, size - overlap)
        for start in range(0, len(words), step):
            chunk = " ".join(words[start:start + size])
            if len(chunk.split()) >= 80:
                chunks.append(chunk)
        return chunks

    @staticmethod
    def _themes(text: str) -> list[str]:
        low = text.lower()
        return [theme for theme, words in THEMES.items() if any(word in low for word in words)]

    def _embedder(self):
        try:
            from sentence_transformers import SentenceTransformer
            return SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
        except Exception as exc:
            self.logger.warning("Sentence transformer unavailable for ideology corpus: %s; storing chunks without embeddings", type(exc).__name__)
            return None

    def fetch(self) -> pl.DataFrame:
        self._ensure_schema()
        raw_dir = ROOT / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        embedder = self._embedder()
        rows = []
        for title, author, frame, url in SOURCES:
            cache = raw_dir / f"ideology_{self._slug(title)}.txt"
            try:
                if not cache.exists():
                    response = httpx.get(url, timeout=90)
                    response.raise_for_status()
                    cache.write_text(response.text, encoding="utf-8", errors="ignore")
                text = self._clean(cache.read_text(encoding="utf-8", errors="ignore"))
            except Exception as exc:
                self.logger.warning("Ideology text fetch failed title=%s url=%s error=%s", title, url, type(exc).__name__)
                continue
            chunks = self._chunks(text)
            embeddings = []
            if embedder and chunks:
                try:
                    embeddings = embedder.encode(chunks, normalize_embeddings=True).tolist()
                except Exception as exc:
                    self.logger.warning("Ideology embedding failed title=%s error=%s", title, type(exc).__name__)
                    embeddings = []
            for index, chunk in enumerate(chunks):
                embedding = embeddings[index] if index < len(embeddings) else None
                chunk_id = hashlib.sha1(f"{title}:{index}:{chunk[:80]}".encode("utf-8")).hexdigest()
                themes = self._themes(chunk)
                rows.append({
                    "chunk_id": chunk_id,
                    "source_title": title,
                    "source_text": title,
                    "author": author,
                    "ideology_frame": frame,
                    "publication_year": None,
                    "source_url": url,
                    "chunk_index": index,
                    "text": chunk,
                    "chunk_text": chunk,
                    "ideology_tags": [frame],
                    "key_themes": themes,
                    "embedding": None if embedding is None else json.dumps(embedding).encode("utf-8"),
                    "word_count": len(chunk.split()),
                    "created_at": datetime.utcnow(),
                    "fetched_at": datetime.utcnow(),
                })
        return pl.DataFrame(rows, infer_schema_length=10000) if rows else pl.DataFrame({"chunk_id": []})

    def run(self) -> bool:
        ok = super().run()
        try:
            df = pl.read_parquet(self.output_path)
            if df.height == 0:
                return ok
            cols = [
                "chunk_id", "source_title", "source_text", "author", "ideology_frame", "publication_year",
                "source_url", "chunk_index", "text", "chunk_text", "ideology_tags", "key_themes",
                "embedding", "word_count", "created_at", "fetched_at",
            ]
            for col in cols:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            self._ensure_schema()
            with write_connection() as con:
                con.register("ideology_df", df.select(cols))
                con.execute(
                    f"""
                    INSERT OR REPLACE INTO ideology_corpus_chunks ({", ".join(cols)})
                    SELECT {", ".join(cols)} FROM ideology_df
                    """
                )
            self.logger.info("Ideology corpus scraper: wrote %s chunks", df.height)
            return ok
        except Exception as exc:
            self.logger.warning("Could not persist ideology corpus chunks: %s", exc)
            return False


if __name__ == "__main__":
    raise SystemExit(0 if IdeologyCorpusScraper().run() else 1)
