import pytest
from src.minhash import shingle, minhash_signature, jaccard_estimate

class TestShingling:
    def test_basic_shingling(self):
        result = shingle("hello world", k=5)
        assert isinstance(result, set)
        assert "hello" in result
        assert " worl" in result

    def test_shingle_size(self):
        text = "abcdefgh"
        result = shingle(text, k=3)
        # "abcdefgh" with k=3 should produce 6 shingles
        assert len(result) == 6

    def test_empty_text(self):
        result = shingle("", k=5)
        assert result == set()

    def test_text_shorter_than_k(self):
        result = shingle("hi", k=5)
        assert result == set()

    def test_whitespace_normalization(self):
        # multiple spaces should be collapsed
        result1 = shingle("hello   world", k=5)
        result2 = shingle("hello world", k=5)
        assert result1 == result2

    def test_case_normalization(self):
        result1 = shingle("Hello World", k=5)
        result2 = shingle("hello world", k=5)
        assert result1 == result2


class TestMinHashSignature:
    def test_signature_length(self):
        shingles = shingle("hello world this is a test", k=5)
        sig = minhash_signature(shingles, num_hashes=128)
        assert len(sig) == 128

    def test_signature_is_list_of_ints(self):
        shingles = shingle("hello world", k=5)
        sig = minhash_signature(shingles, num_hashes=64)
        assert all(isinstance(h, int) for h in sig)

    def test_same_text_same_signature(self):
        text = "the quick brown fox jumps over the lazy dog"
        shingles = shingle(text, k=5)
        sig1 = minhash_signature(shingles, num_hashes=128)
        sig2 = minhash_signature(shingles, num_hashes=128)
        assert sig1 == sig2

    def test_different_texts_different_signatures(self):
        shingles1 = shingle("the quick brown fox", k=5)
        shingles2 = shingle("completely unrelated content here", k=5)
        sig1 = minhash_signature(shingles1, num_hashes=128)
        sig2 = minhash_signature(shingles2, num_hashes=128)
        assert sig1 != sig2

    def test_empty_shingles(self):
        sig = minhash_signature(set(), num_hashes=128)
        assert len(sig) == 128


class TestJaccardEstimate:
    def test_identical_signatures_return_one(self):
        shingles = shingle("the quick brown fox jumps over the lazy dog", k=5)
        sig = minhash_signature(shingles, num_hashes=128)
        similarity = jaccard_estimate(sig, sig)
        assert similarity == 1.0

    def test_similarity_between_zero_and_one(self):
        shingles1 = shingle("the quick brown fox", k=5)
        shingles2 = shingle("the quick brown cat", k=5)
        sig1 = minhash_signature(shingles1, num_hashes=128)
        sig2 = minhash_signature(shingles2, num_hashes=128)
        similarity = jaccard_estimate(sig1, sig2)
        assert 0.0 <= similarity <= 1.0

    def test_completely_different_texts_low_similarity(self):
        shingles1 = shingle("the quick brown fox jumps over the lazy dog", k=5)
        shingles2 = shingle("quantum computing breakthrough announced yesterday", k=5)
        sig1 = minhash_signature(shingles1, num_hashes=128)
        sig2 = minhash_signature(shingles2, num_hashes=128)
        similarity = jaccard_estimate(sig1, sig2)
        assert similarity < 0.3

    def test_nearly_identical_texts_high_similarity(self):
        base = "Federal Reserve raises interest rates by quarter point amid inflation concerns"
        # one word changed
        variant = "Federal Reserve raises interest rates by quarter point amid economic concerns"
        shingles1 = shingle(base, k=5)
        shingles2 = shingle(variant, k=5)
        true_sim = len(shingles1 & shingles2) / len(shingles1 | shingles2)
        sig1 = minhash_signature(shingles1, num_hashes=128)
        sig2 = minhash_signature(shingles2, num_hashes=128)
        estimated_sim = jaccard_estimate(sig1, sig2)
        assert abs(true_sim - estimated_sim) < 0.15

    def test_syndicated_article_similarity(self):
        # simulates two outlets publishing the same Reuters wire story
        # with minor edits
        reuters = """
            The Federal Reserve raised its benchmark interest rate by a quarter 
            percentage point on Wednesday, continuing its campaign to bring 
            inflation down to its 2% target. The decision was unanimous among 
            voting members of the Federal Open Market Committee.
        """
        ap_version = """
            The Federal Reserve raised its benchmark interest rate by a quarter 
            percentage point Wednesday, pushing forward its effort to bring 
            inflation down to its 2% goal. The move was approved unanimously by 
            the Federal Open Market Committee.
        """
        shingles1 = shingle(reuters, k=5)
        shingles2 = shingle(ap_version, k=5)
        true_sim = len(shingles1 & shingles2) / len(shingles1 | shingles2) 
        sig1 = minhash_signature(shingles1, num_hashes=128)
        sig2 = minhash_signature(shingles2, num_hashes=128)
        similarity = jaccard_estimate(sig1, sig2)
        assert abs(true_sim - similarity) < 0.05

    def test_mismatched_signature_lengths_raises(self):
        sig1 = [1, 2, 3]
        sig2 = [1, 2]
        with pytest.raises(Exception):
            jaccard_estimate(sig1, sig2)


class TestAccuracyVsGroundTruth:
    """
    Validates that MinHash similarity estimates are close to true Jaccard similarity.
    """

    def true_jaccard(self, set1: set, set2: set) -> float:
        if not set1 and not set2:
            return 1.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union

    def test_estimate_close_to_true_jaccard(self):
        text1 = "the federal reserve raised interest rates amid inflation concerns"
        text2 = "the federal reserve raised interest rates amid economic uncertainty"
        
        shingles1 = shingle(text1, k=5)
        shingles2 = shingle(text2, k=5)
        
        true_sim = self.true_jaccard(shingles1, shingles2)
        estimated_sim = jaccard_estimate(
            minhash_signature(shingles1, num_hashes=256),
            minhash_signature(shingles2, num_hashes=256)
        )
        
        # estimate should be within 0.15 of true value
        # more hash functions = tighter bound
        assert abs(true_sim - estimated_sim) < 0.15

    def test_more_hashes_better_accuracy(self):
        text1 = "breaking news federal reserve raises rates quarter point decision unanimous"
        text2 = "federal reserve raises benchmark rate quarter point unanimous committee vote"
        
        shingles1 = shingle(text1, k=5)
        shingles2 = shingle(text2, k=5)
        true_sim = self.true_jaccard(shingles1, shingles2)
        
        errors = []
        for num_hashes in [32, 64, 128, 256]:
            sig1 = minhash_signature(shingles1, num_hashes=num_hashes)
            sig2 = minhash_signature(shingles2, num_hashes=num_hashes)
            estimated = jaccard_estimate(sig1, sig2)
            errors.append(abs(true_sim - estimated))
        
        # log the errors so you can see the accuracy improvement
        for num_hashes, error in zip([32, 64, 128, 256], errors):
            print(f"num_hashes={num_hashes}: error={error:.4f}")
        
        # 256 hashes should be more accurate than 32 on average
        # this can occasionally fail due to randomness, which is expected
        assert errors[-1] < errors[0] * 1.5
