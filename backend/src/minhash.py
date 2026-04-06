import re
import hashlib

def shingle(text: str, k: int = 5) -> set[str]:
    text = re.sub(r'\s+', ' ', text.lower().strip())
    return {text[i:i+k] for i in range(len(text) - k + 1)}

def minhash_signature(shingles: set[str], num_hashes: int = 128) -> list[str]:
    if not shingles:
        return [0] * num_hashes

    signature = []
    for seed in range(num_hashes):
        seed_hash = int(hashlib.md5(seed.to_bytes(4)).hexdigest(), 16)
        min_hash = min(
            int(hashlib.md5(s.encode()).hexdigest(), 16) ^ seed_hash
            for s in shingles
        )
        signature.append(min_hash)
    return signature

def jaccard_estimate(sig1: list[str], sig2: list[str]) -> float:
    """Return probability that two signatures are equal"""
    if len(sig1) != len(sig2):
        raise Exception(f"sig1 has length {len(sig1)} while sig2 has length {len(sig2)}")

    matches = sum(a == b for a, b in zip(sig1, sig2))
    return matches / len(sig1)

def true_jaccard(set1, set2):
    if not set1 and not set2:
        return 1.0
    return len(set1 & set2) / len(set1 | set2)

# add this temporarily to your test
base = "Federal Reserve raises interest rates by quarter point amid inflation concerns"
variant = "Federal Reserve raises interest rates by quarter point amid economic concerns"
s1 = shingle(base, k=5)
s2 = shingle(variant, k=5)
print(f"True Jaccard: {true_jaccard(s1, s2)}")
print(f"Shingle counts: {len(s1)}, {len(s2)}")
