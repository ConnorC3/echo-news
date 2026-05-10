import pytest
from src.minhash.minhash import shingle
from src.minhash.minhash_scratch import minhash_signature, jaccard_estimate
from datasketch import MinHash as DatasketchMinHash

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

class TestAgainstDatasketch:
    """
    Validates that my scratch implementation produces similarity estimates
    consistent with datasketch. Don't expect identical values, since different
    hash functions produce different absolute signatures, but the relative
    similarity ordering and approximate magnitudes should agree.
    """

    def datasketch_similarity(self, text1: str, text2: str, num_perm: int = 128) -> float:
        m1 = DatasketchMinHash(num_perm=num_perm)
        m2 = DatasketchMinHash(num_perm=num_perm)
        
        for s in shingle(text1, k=5):
            m1.update(s.encode())
        for s in shingle(text2, k=5):
            m2.update(s.encode())
        
        return m1.jaccard(m2)

    def scratch_similarity(self, text1: str, text2: str, num_hashes: int = 128) -> float:
        sig1 = minhash_signature(shingle(text1, k=5), num_hashes=num_hashes)
        sig2 = minhash_signature(shingle(text2, k=5), num_hashes=num_hashes)
        return jaccard_estimate(sig1, sig2)

    def true_jaccard(self, text1: str, text2: str) -> float:
        s1 = shingle(text1, k=5)
        s2 = shingle(text2, k=5)
        if not s1 and not s2:
            return 1.0
        return len(s1 & s2) / len(s1 | s2)

    def test_both_close_to_true_jaccard_identical_texts(self):
        text = "the federal reserve raised interest rates amid inflation concerns"
        true_sim = self.true_jaccard(text, text)
        scratch_sim = self.scratch_similarity(text, text)
        datasketch_sim = self.datasketch_similarity(text, text)
        
        assert abs(true_sim - scratch_sim) < 0.05
        assert abs(true_sim - datasketch_sim) < 0.05

    def test_both_agree_on_similar_texts(self):
        text1 = """
            The Federal Reserve raised its benchmark interest rate by a quarter
            percentage point on Wednesday, continuing its campaign to bring
            inflation down to its 2 percent target. The decision was unanimous
            among voting members of the Federal Open Market Committee.
        """
        text2 = """
            The Federal Reserve raised its benchmark interest rate by a quarter
            percentage point Wednesday, pushing forward its effort to bring
            inflation down to its 2 percent goal. The move was approved unanimously
            by the Federal Open Market Committee.
        """
        
        true_sim = self.true_jaccard(text1, text2)
        scratch_sim = self.scratch_similarity(text1, text2)
        datasketch_sim = self.datasketch_similarity(text1, text2)
        
        # both should be within 0.15 of true jaccard
        assert abs(true_sim - scratch_sim) < 0.15
        assert abs(true_sim - datasketch_sim) < 0.15
        
        # both should agree with each other within reasonable tolerance
        # they use different hash functions so won't be identical
        assert abs(scratch_sim - datasketch_sim) < 0.2

    def test_both_agree_on_dissimilar_texts(self):
        text1 = "federal reserve raises interest rates inflation monetary policy"
        text2 = "quantum computing breakthrough researchers silicon valley startup"
        
        scratch_sim = self.scratch_similarity(text1, text2)
        datasketch_sim = self.datasketch_similarity(text1, text2)
        
        # both should identify these as dissimilar
        assert scratch_sim < 0.3
        assert datasketch_sim < 0.3

    def test_ordering_is_consistent(self):
        """
        Most important test: both implementations should agree on which
        pair is more similar, even if absolute values differ.
        """
        base = """
            The Federal Reserve raised interest rates by a quarter point
            amid ongoing inflation concerns at its Wednesday meeting.
        """
        similar = """
            The Federal Reserve raised interest rates by a quarter point
            amid persistent inflation worries at its meeting Wednesday.
        """
        dissimilar = """
            Apple announced record quarterly earnings driven by strong
            iPhone sales in emerging markets despite supply chain concerns.
        """
        
        scratch_sim_high = self.scratch_similarity(base, similar)
        scratch_sim_low = self.scratch_similarity(base, dissimilar)
        datasketch_sim_high = self.datasketch_similarity(base, similar)
        datasketch_sim_low = self.datasketch_similarity(base, dissimilar)
        
        # both implementations should agree that base/similar > base/dissimilar
        assert scratch_sim_high > scratch_sim_low
        assert datasketch_sim_high > datasketch_sim_low

    def test_accuracy_improves_with_more_hashes(self):
        """
        Both implementations should show decreasing error as num_hashes grows.
        Validates the law of large numbers property of MinHash.
        """
        text1 = """
            The Federal Reserve raised its benchmark interest rate by a quarter
            percentage point on Wednesday, continuing its campaign to bring
            inflation down to its 2 percent target. The decision was unanimous
            among voting members of the Federal Open Market Committee. Chair
            Jerome Powell said the central bank remains committed to restoring
            price stability.
        """
        text2 = """
            The Federal Reserve raised its benchmark interest rate by a quarter
            percentage point Wednesday, pushing forward its effort to bring
            inflation down to its 2 percent goal. The move was approved unanimously
            by the Federal Open Market Committee. Fed Chair Jerome Powell indicated
            the central bank is committed to bringing down inflation.
        """
        
        true_sim = self.true_jaccard(text1, text2)
        
        print(f"\nTrue Jaccard: {true_sim:.4f}")
        print(f"{'num_hashes':<12} {'scratch error':<16} {'datasketch error':<16}")
        
        scratch_errors = []
        datasketch_errors = []
        
        for num_hashes in [16, 32, 64, 128, 256]:
            scratch_sim = self.scratch_similarity(text1, text2, num_hashes)
            datasketch_sim = self.datasketch_similarity(text1, text2, num_hashes)
            
            scratch_err = abs(true_sim - scratch_sim)
            datasketch_err = abs(true_sim - datasketch_sim)
            
            scratch_errors.append(scratch_err)
            datasketch_errors.append(datasketch_err)
            
            print(f"{num_hashes:<12} {scratch_err:<16.4f} {datasketch_err:<16.4f}")
        
        # at 256 hashes both should be reasonably accurate
        assert scratch_errors[-1] < 0.15
        assert datasketch_errors[-1] < 0.15
