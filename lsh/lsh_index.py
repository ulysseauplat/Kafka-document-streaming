from typing import List, Dict, Set, Tuple
from collections import defaultdict
import mmh3

class LSHIndex:
    """Locality Sensitive Hashing index using banding technique."""
    def __init__(self, r: int, b: int):
        self.r = r
        self.b = b
        self.buckets: List[Dict[int, List[int]]] = [defaultdict(list) for _ in range(b)]
        self.candidate_pairs: Set[Tuple[int, int]] = set()

    def insert(self, signature: List[int], comment_idx: int) -> None:
        """Insert a signature and find candidate similar pairs."""
        for band_idx in range(self.b):
            start = band_idx * self.r
            end = start + self.r
            band_signature = signature[start:end]
            bucket_id = mmh3.hash64(bytes(str(band_signature), "utf-8"), signed=False)[0]
            bucket = self.buckets[band_idx][bucket_id]
            for existing_idx in bucket:
                pair = tuple(sorted((existing_idx, comment_idx)))
                self.candidate_pairs.add(pair)
            bucket.append(comment_idx)
