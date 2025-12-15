from typing import List, Union
import numpy as np
from sentence_transformers import SentenceTransformer

from lib.core.container import container


class EmbeddingService:
    """
    Service for computing text embeddings using sentence-transformers.
    Model is loaded once at initialization and reused.
    """

    def __init__(self, model_name: str | None = None):
        self.model_name = model_name or container.settings.EMBEDDING_MODEL
        self._model: SentenceTransformer | None = None
        self._embedding_dim: int | None = None
        self._logger = container.logger

    @property
    def model(self) -> SentenceTransformer:
        """Gets model instance, loads on first access."""
        self._load_model()
        return self._model

    @property
    def embedding_dimension(self) -> int:
        """Embedding dimensionality (384 for all-MiniLM-L6-v2)."""
        self._load_model()
        return self._embedding_dim

    def encode(
        self,
        texts: Union[str, List[str]],
        batch_size: int = 32,
        show_progress: bool = False,
        normalize: bool = True
    ) -> np.ndarray:
        """Computes embeddings for text or list of texts."""
        if isinstance(texts, str):
            texts = [texts]
            single_input = True
        else:
            single_input = False

        valid_texts = [t if t else "" for t in texts]

        try:
            embeddings = self.model.encode(
                valid_texts,
                batch_size=batch_size,
                show_progress_bar=show_progress,
                normalize_embeddings=normalize,
                convert_to_numpy=True
            )

            if single_input:
                return embeddings[0]

            return embeddings

        except Exception as e:
            self._logger.error(f"Encoding failed for {len(texts)} texts: {e}")
            raise

    def compute_similarity(self, embedding1: np.ndarray, embedding2: np.ndarray) -> float:
        """Computes cosine similarity between two embeddings."""
        similarity = np.dot(embedding1, embedding2)

        return float(similarity)

    def encode_dataset_metadata(self, title: str, description: str | None = None) -> np.ndarray:
        """
        Creates embedding from dataset metadata.
        Title is weighted more heavily by including it twice.
        """
        text_parts = [title, title]

        if description:
            text_parts.append(description)

        combined_text = " ".join(text_parts)
        return self.encode(combined_text)

    def _load_model(self) -> None:
        """Lazy model loading on first use."""
        if self._model is not None:
            return

        try:
            self._logger.info(f"Loading embedding model: {self.model_name}")
            self._model = SentenceTransformer(self.model_name)
            self._embedding_dim = self._model.get_sentence_embedding_dimension()
            self._logger.info(f"Model loaded. Embedding dimension: {self._embedding_dim}")

        except Exception as e:
            self._logger.error(f"Failed to load model {self.model_name}: {e}")
            raise RuntimeError(f"Cannot load embedding model: {e}") from e
