import random
from typing import List, Tuple, Set
from shared.config import PRIME

# Generate parameters for hash functions so they remain consistent
# Hash functions are of the form: f(x) = a*x + b mod(prime)
# Returns an array with (a, b) parameters for each hash function

def generate_hash_params(n_hash: int, prime: int) -> List[Tuple[int, int]]:
    """Generate parameters for MinHash functions."""
    return [(random.randint(1, prime - 1), random.randint(0, prime - 1)) for _ in range(n_hash)]


# Create a signature array with the same number of elements as hash functions
# Iterate over all shingles, hash them with each function, keep the minimum
def compute_minhash_signature(
    shingles: Set[int], hash_params: List[Tuple[int, int]], prime: int
) -> List[int]:
    """Compute MinHash signature of a set of shingles."""
    signature = [float("inf")] * len(hash_params)
    for shingle_id in sorted(shingles):
        for i, (a, b) in enumerate(hash_params):
            h = (a * shingle_id + b) % PRIME
            if h < signature[i]:
                signature[i] = h
    return signature
