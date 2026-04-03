from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class RetrievalSnippet:
    source: str
    path: str
    content: str
    score: float
    lexical_score: float = 0.0
    semantic_score: float = 0.0
    fused_score: float = 0.0


@dataclass(slots=True)
class RetrievalChunk:
    chunk_id: str
    source: str
    path: str
    content: str
    embedding: list[float] | None = None


@dataclass(slots=True)
class RetrievalService:
    root: Path
    embedding_model: str = "text-embedding-3-small"
    chunk_chars: int = 1200
    chunk_overlap: int = 180
    candidate_k: int = 24
    corpus: list[tuple[str, Path, str]] = field(default_factory=list)
    chunks: list[RetrievalChunk] = field(default_factory=list)
    _doc_freq: Counter[str] = field(default_factory=Counter)
    _avg_doc_len: float = 1.0

    def load_corpus(self, candidates: list[tuple[str, Path]]) -> None:
        self.corpus.clear()
        for source, path in candidates:
            if path.exists():
                self.corpus.append((source, path, path.read_text(encoding="utf-8")))
        self._prepare_index()

    def load_default_corpus(self) -> None:
        self.load_corpus(
            [
                ("evidence", self.root / "evidence/pages/sales.md"),
                ("evidence", self.root / "evidence/pages/customers.md"),
                ("evidence", self.root / "evidence/pages/products.md"),
                ("dbt", self.root / "dbt_project/models/marts/fct_orders.sql"),
                ("dbt", self.root / "dbt_project/models/staging/stg_pageviews.sql"),
                ("dbt", self.root / "dbt_project/models/staging/_sources.yml"),
            ]
        )

    def retrieve_context(self, question: str, limit: int = 6) -> list[RetrievalSnippet]:
        if not self.chunks:
            return []

        lexical = self._lexical_scores(question)
        semantic = self._semantic_scores(question)
        fused = self._reciprocal_rank_fusion(lexical, semantic)

        candidates = sorted(
            self.chunks,
            key=lambda c: fused.get(c.chunk_id, 0.0),
            reverse=True,
        )[: max(self.candidate_k, limit * 3)]
        selected = self._mmr_select(question, candidates, fused, limit=limit)

        snippets: list[RetrievalSnippet] = []
        for chunk in selected:
            snippets.append(
                RetrievalSnippet(
                    source=chunk.source,
                    path=chunk.path,
                    content=chunk.content,
                    score=fused.get(chunk.chunk_id, 0.0),
                    lexical_score=lexical.get(chunk.chunk_id, 0.0),
                    semantic_score=semantic.get(chunk.chunk_id, 0.0),
                    fused_score=fused.get(chunk.chunk_id, 0.0),
                )
            )
        return snippets

    def _prepare_index(self) -> None:
        cache_path = self.root / "agent/cache/rag_index.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = self._manifest()
        cached = self._load_cache(cache_path, manifest)
        if cached:
            self.chunks = cached
            self._build_doc_frequency()
            return

        built: list[RetrievalChunk] = []
        for source, path, content in self.corpus:
            rel = str(path.relative_to(self.root))
            for idx, chunk_text in enumerate(self._chunk_text(content)):
                built.append(
                    RetrievalChunk(
                        chunk_id=f"{rel}::chunk-{idx}",
                        source=source,
                        path=rel,
                        content=chunk_text,
                    )
                )

        self._embed_chunks(built)
        self.chunks = built
        self._build_doc_frequency()
        self._save_cache(cache_path, manifest, built)

    def _manifest(self) -> list[dict[str, object]]:
        entries: list[dict[str, object]] = []
        for _, path, _ in self.corpus:
            stat = path.stat()
            entries.append(
                {
                    "path": str(path.relative_to(self.root)),
                    "size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                }
            )
        entries.sort(key=lambda x: str(x["path"]))
        return entries

    def _load_cache(self, cache_path: Path, manifest: list[dict[str, object]]) -> list[RetrievalChunk] | None:
        if not cache_path.exists():
            return None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if payload.get("embedding_model") != self.embedding_model:
            return None
        if payload.get("manifest") != manifest:
            return None
        chunks: list[RetrievalChunk] = []
        for item in payload.get("chunks", []):
            chunks.append(
                RetrievalChunk(
                    chunk_id=item["chunk_id"],
                    source=item["source"],
                    path=item["path"],
                    content=item["content"],
                    embedding=item.get("embedding"),
                )
            )
        return chunks

    def _save_cache(self, cache_path: Path, manifest: list[dict[str, object]], chunks: list[RetrievalChunk]) -> None:
        payload = {
            "embedding_model": self.embedding_model,
            "manifest": manifest,
            "chunks": [
                {
                    "chunk_id": c.chunk_id,
                    "source": c.source,
                    "path": c.path,
                    "content": c.content,
                    "embedding": c.embedding,
                }
                for c in chunks
            ],
        }
        cache_path.write_text(json.dumps(payload), encoding="utf-8")

    def _chunk_text(self, text: str) -> list[str]:
        normalized = text.strip()
        if not normalized:
            return []
        chunks: list[str] = []
        start = 0
        step = max(self.chunk_chars - self.chunk_overlap, 200)
        while start < len(normalized):
            end = min(start + self.chunk_chars, len(normalized))
            piece = normalized[start:end].strip()
            if piece:
                chunks.append(piece)
            if end >= len(normalized):
                break
            start += step
        return chunks

    def _embed_chunks(self, chunks: list[RetrievalChunk]) -> None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key or not chunks:
            return
        try:
            from openai import OpenAI

            client = OpenAI(api_key=api_key)
            batch_size = 64
            for i in range(0, len(chunks), batch_size):
                batch = chunks[i : i + batch_size]
                texts = [c.content[:6000] for c in batch]
                response = client.embeddings.create(model=self.embedding_model, input=texts)
                for item, chunk in zip(response.data, batch, strict=False):
                    chunk.embedding = item.embedding
        except Exception:
            # Keep lexical retrieval available even if embeddings fail.
            return

    def _tokenize(self, text: str) -> list[str]:
        return [t for t in re.findall(r"[a-zA-Z_][a-zA-Z0-9_]+", text.lower()) if len(t) > 2]

    def _build_doc_frequency(self) -> None:
        self._doc_freq.clear()
        lengths: list[int] = []
        for chunk in self.chunks:
            tokens = self._tokenize(chunk.content)
            lengths.append(len(tokens))
            seen = set(tokens)
            for token in seen:
                self._doc_freq[token] += 1
        self._avg_doc_len = max((sum(lengths) / len(lengths)) if lengths else 1.0, 1.0)

    def _lexical_scores(self, question: str) -> dict[str, float]:
        query_tokens = self._tokenize(question)
        if not query_tokens:
            return {}
        n_docs = max(len(self.chunks), 1)
        k1 = 1.5
        b = 0.75
        scores: dict[str, float] = {}
        for chunk in self.chunks:
            tokens = self._tokenize(chunk.content)
            if not tokens:
                continue
            tf = Counter(tokens)
            dl = len(tokens)
            score = 0.0
            for term in query_tokens:
                if term not in tf:
                    continue
                df = self._doc_freq.get(term, 0)
                idf = math.log(1 + (n_docs - df + 0.5) / (df + 0.5))
                term_tf = tf[term]
                denom = term_tf + k1 * (1 - b + b * (dl / self._avg_doc_len))
                score += idf * ((term_tf * (k1 + 1)) / max(denom, 1e-9))
            if score > 0:
                scores[chunk.chunk_id] = score
        return scores

    def _semantic_scores(self, question: str) -> dict[str, float]:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return {}
        embedded_chunks = [c for c in self.chunks if c.embedding]
        if not embedded_chunks:
            return {}
        try:
            from openai import OpenAI

            client = OpenAI(api_key=api_key)
            q_vec = client.embeddings.create(
                model=self.embedding_model,
                input=[question[:6000]],
            ).data[0].embedding
        except Exception:
            return {}

        scores: dict[str, float] = {}
        q_norm = math.sqrt(sum(v * v for v in q_vec)) or 1.0
        for chunk in embedded_chunks:
            c_vec = chunk.embedding or []
            c_norm = math.sqrt(sum(v * v for v in c_vec)) or 1.0
            dot = sum(a * b for a, b in zip(q_vec, c_vec, strict=False))
            scores[chunk.chunk_id] = dot / (q_norm * c_norm)
        return scores

    def _reciprocal_rank_fusion(
        self,
        lexical: dict[str, float],
        semantic: dict[str, float],
        k: int = 60,
    ) -> dict[str, float]:
        fused: dict[str, float] = {}
        for scores in (lexical, semantic):
            ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            for rank, (chunk_id, _) in enumerate(ranked, start=1):
                fused[chunk_id] = fused.get(chunk_id, 0.0) + 1.0 / (k + rank)
        return fused

    def _mmr_select(
        self,
        question: str,
        candidates: list[RetrievalChunk],
        fused: dict[str, float],
        limit: int,
        lambda_mult: float = 0.75,
    ) -> list[RetrievalChunk]:
        if len(candidates) <= limit:
            return candidates
        by_id = {c.chunk_id: c for c in candidates}
        selected: list[RetrievalChunk] = []
        remaining = set(by_id.keys())

        while remaining and len(selected) < limit:
            best_id = None
            best_score = -1e9
            for cid in remaining:
                rel = fused.get(cid, 0.0)
                div_penalty = 0.0
                for chosen in selected:
                    div_penalty = max(div_penalty, self._chunk_similarity(by_id[cid], chosen))
                mmr = lambda_mult * rel - (1 - lambda_mult) * div_penalty
                if mmr > best_score:
                    best_score = mmr
                    best_id = cid
            if best_id is None:
                break
            selected.append(by_id[best_id])
            remaining.remove(best_id)
        return selected

    def _chunk_similarity(self, a: RetrievalChunk, b: RetrievalChunk) -> float:
        if a.embedding and b.embedding:
            an = math.sqrt(sum(v * v for v in a.embedding)) or 1.0
            bn = math.sqrt(sum(v * v for v in b.embedding)) or 1.0
            dot = sum(x * y for x, y in zip(a.embedding, b.embedding, strict=False))
            return dot / (an * bn)
        # Lexical fallback when embeddings are unavailable.
        at = set(self._tokenize(a.content))
        bt = set(self._tokenize(b.content))
        if not at or not bt:
            return 0.0
        return len(at & bt) / len(at | bt)

