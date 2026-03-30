import pytest
from lsh.preprocess import preprocess, get_word_shingles, process_comment
from lsh.minhash import generate_hash_params, compute_minhash_signature
from lsh.lsh_index import LSHIndex
from lsh.similarity import jaccard_similarity, compute_true_pairs


class TestPreprocess:
    def test_preprocess_lowercase(self):
        result = preprocess("Hello WORLD", k=2)
        assert result == "hello world"

    def test_preprocess_removes_mentions(self):
        result = preprocess("Hello @john how are you", k=2)
        assert "@john" not in result

    def test_preprocess_removes_urls(self):
        result = preprocess("Check https://example.com for more info", k=2)
        assert "https://example.com" not in result
        assert "www.test.org" not in preprocess("Visit www.test.org today", k=2)

    def test_preprocess_removes_punctuation(self):
        result = preprocess("Hello, world! How are you?", k=2)
        assert "," not in result
        assert "!" not in result
        assert "?" not in result

    def test_preprocess_normalizes_whitespace(self):
        result = preprocess("Hello    world   foo", k=2)
        assert "  " not in result

    def test_preprocess_returns_none_for_short_text(self):
        result = preprocess("Hello", k=4)
        assert result is None

    def test_preprocess_handles_non_string(self):
        result = preprocess(123, k=2)
        assert result is None

    def test_preprocess_exact_k_words_returns_text(self):
        result = preprocess("one two three four", k=4)
        assert result is not None


class TestShingling:
    def test_shingling_generates_k_shingles(self):
        text = "the quick brown fox jumps"
        shingles = get_word_shingles(text, k=3)
        words = text.split()
        expected_count = len(words) - 3 + 1
        assert len(shingles) == expected_count

    def test_shingling_same_input_same_output(self):
        text = "the quick brown fox"
        shingles1 = get_word_shingles(text, k=2)
        shingles2 = get_word_shingles(text, k=2)
        assert shingles1 == shingles2

    def test_shingling_different_k_different_output(self):
        text = "the quick brown fox jumps"
        shingles_k2 = get_word_shingles(text, k=2)
        shingles_k3 = get_word_shingles(text, k=3)
        assert len(shingles_k2) != len(shingles_k3)


class TestProcessComment:
    def test_process_comment_full_pipeline(self):
        result = process_comment("Check https://example.com @user hello world foo", k=2)
        assert result is not None
        assert isinstance(result, set)
        assert len(result) > 0

    def test_process_comment_filters_short(self):
        result = process_comment("hi", k=3)
        assert result is None


class TestMinHash:
    def test_generate_hash_params_length(self):
        params = generate_hash_params(50, 2147483647)
        assert len(params) == 50

    def test_generate_hash_params_format(self):
        params = generate_hash_params(10, 2147483647)
        for a, b in params:
            assert isinstance(a, int)
            assert isinstance(b, int)
            assert 1 <= a < 2147483647
            assert 0 <= b < 2147483647

    def test_minhash_same_input_same_signature(self):
        shingles = {1, 2, 3, 4, 5}
        params = generate_hash_params(50, 2147483647)
        sig1 = compute_minhash_signature(shingles, params, 2147483647)
        sig2 = compute_minhash_signature(shingles, params, 2147483647)
        assert sig1 == sig2

    def test_minhash_signature_length_matches_params(self):
        shingles = {1, 2, 3, 4, 5}
        params = generate_hash_params(50, 2147483647)
        sig = compute_minhash_signature(shingles, params, 2147483647)
        assert len(sig) == 50

    def test_minhash_signature_values_are_integers(self):
        shingles = {1, 2, 3, 4, 5}
        params = generate_hash_params(50, 2147483647)
        sig = compute_minhash_signature(shingles, params, 2147483647)
        for val in sig:
            assert isinstance(val, int)


class TestLSHIndex:
    def test_lsh_insert_adds_to_buckets(self):
        lsh = LSHIndex(r=5, b=10)
        signature = [i for i in range(50)]
        lsh.insert(signature, 0)
        total_items = sum(len(bucket) for band in lsh.buckets for bucket in band.values())
        assert total_items == 10

    def test_lsh_insert_same_signature_creates_candidate(self):
        lsh = LSHIndex(r=5, b=10)
        signature = [1] * 50
        lsh.insert(signature, 0)
        lsh.insert(signature, 1)
        assert len(lsh.candidate_pairs) > 0

    def test_lsh_insert_different_signature_no_candidate(self):
        lsh = LSHIndex(r=5, b=10)
        sig1 = [1] * 50
        sig2 = [2] * 50
        lsh.insert(sig1, 0)
        lsh.insert(sig2, 1)
        assert len(lsh.candidate_pairs) == 0


class TestJaccardSimilarity:
    def test_jaccard_identical_sets(self):
        s1 = {1, 2, 3}
        s2 = {1, 2, 3}
        assert jaccard_similarity(s1, s2) == 1.0

    def test_jaccard_disjoint_sets(self):
        s1 = {1, 2, 3}
        s2 = {4, 5, 6}
        assert jaccard_similarity(s1, s2) == 0.0

    def test_jaccard_partial_overlap(self):
        s1 = {1, 2, 3}
        s2 = {2, 3, 4}
        expected = 2 / 4
        assert jaccard_similarity(s1, s2) == expected

    def test_jaccard_empty_sets(self):
        assert jaccard_similarity(set(), set()) == 0.0


class TestComputeTruePairs:
    def test_compute_true_pairs_above_threshold(self):
        candidates = {(0, 1)}
        shingle_dict = {
            0: {1, 2, 3, 4},
            1: {1, 2, 3, 5}
        }
        result = compute_true_pairs(candidates, shingle_dict, threshold=0.5)
        assert len(result) == 1
        assert result[0][2] >= 0.5

    def test_compute_true_pairs_below_threshold(self):
        candidates = {(0, 1)}
        shingle_dict = {
            0: {1, 2, 3},
            1: {4, 5, 6}
        }
        result = compute_true_pairs(candidates, shingle_dict, threshold=0.5)
        assert len(result) == 0

    def test_compute_true_pairs_multiple_pairs(self):
        candidates = {(0, 1), (2, 3)}
        shingle_dict = {
            0: {1, 2, 3},
            1: {1, 2, 3},
            2: {4, 5, 6},
            3: {7, 8, 9}
        }
        result = compute_true_pairs(candidates, shingle_dict, threshold=0.5)
        assert len(result) == 1
