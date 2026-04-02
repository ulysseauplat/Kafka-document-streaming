import re
import string
from typing import Optional

import mmh3

# Clean and normalize comment:
# - Lowercase
# - Remove @mentions
# - Remove URLs
# - Remove punctuation
# - Collapse whitespace
# - Reject comments with fewer than k words

def preprocess(comment: str, k: int) -> Optional[str]:
    """Clean and normalize a comment before shingling."""
    if not isinstance(comment, str):
        return None
    comment = comment.lower()
    comment = re.sub(r"@\w+", "", comment)
    comment = re.sub(r"(https?://\S+|www\.\S+)", "", comment)
    comment = comment.translate(str.maketrans("", "", string.punctuation))
    comment = re.sub(r"\s+", " ", comment).strip()
    if len(comment.split()) < k:
        return None
    return comment

# Split comment into words, create k-word shingles, hash them
# Returns a set of hashed shingles for the input comment
def get_word_shingles(comment: str, k: int) -> set[int]:
    """Generate k-word shingles and hash them."""
    words = comment.split()
    shingles = set()
    for i in range(len(words) - k + 1):
        shingle = " ".join(words[i:i + k])
        shingles.add(mmh3.hash(shingle, signed=False))
    return shingles

# Full preprocessing pipeline:
# 1. Clean the comment
# 2. Generate and hash shingles
def process_comment(comment: str, k: int) -> Optional[set[int]]:
    """Full preprocessing pipeline for a comment."""
    comment_clean = preprocess(comment, k)
    if comment_clean is None:
        return None
    shingles = get_word_shingles(comment_clean, k)
    return shingles if shingles else None
