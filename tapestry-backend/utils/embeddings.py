from __future__ import annotations

import numpy as np

_model = None


def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def embed_text(text: str) -> bytes:
    clean = " ".join(str(text or "").split())
    if not clean:
        clean = "empty"
    vector = get_model().encode(clean[:4000], normalize_embeddings=True)
    return vector.astype(np.float32).tobytes()


def deserialize(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32)


def cosine_sim(left: bytes, right: bytes) -> float:
    a = deserialize(left)
    b = deserialize(right)
    denom = float(np.linalg.norm(a) * np.linalg.norm(b)) + 1e-9
    return float(np.dot(a, b) / denom)
