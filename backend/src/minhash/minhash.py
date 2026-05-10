import re
from datasketch import MinHash
import numpy as np

def shingle(text: str, k: int = 5) -> set[str]:
    text = re.sub(r'\s+', ' ', text.lower().strip())
    return {text[i:i+k] for i in range(len(text) - k + 1)}

def compute_minhash(content: str | None, num_perm: int = 128) -> bytes | None:
    if not content or len(content) < 200:
        return None
    
    shingles = shingle(content)

    if len(shingles) < 20:
        return None

    m = MinHash(num_perm=num_perm)
    for s in shingles:
        m.update(s.encode())
    return m.hashvalues.tobytes()

def bytes_to_hashvalues(sig: bytes) -> np.ndarray:
    return np.frombuffer(sig, dtype=np.uint64)

def jaccard_from_bytes(sig1_bytes: bytes, sig2_bytes: bytes) -> float:
    sig1 = bytes_to_hashvalues(sig1_bytes)
    sig2 = bytes_to_hashvalues(sig2_bytes)

    return float(np.sum(sig1 == sig2) / len(sig1))
