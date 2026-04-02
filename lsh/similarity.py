

def jaccard_similarity(s1: set[int], s2: set[int]) -> float:
    """Compute Jaccard similarity between two sets."""
    union = len(s1 | s2)
    return len(s1 & s2) / union if union else 0.0   # In case the union is empty which means the shingles are empty then we return 0 for no similarity

def compute_true_pairs(
    candidate_pairs: set[tuple[int, int]],
    shingle_dict: dict[int, set[int]],
    threshold: float
) -> list[tuple[int, int, float]]:
    """Compute exact Jaccard similarity for candidate pairs."""
    true_pairs = []
    for c1, c2 in candidate_pairs:      # we iterate over all the candidate pairs to check if the Jaccard similarity indeed is above our threshold or not
        jaccard = jaccard_similarity(shingle_dict[c1], shingle_dict[c2])
        if jaccard >= threshold:
            true_pairs.append((c1, c2, jaccard))
    return true_pairs
