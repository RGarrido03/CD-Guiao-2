def dht_hash(text, seed=0, maximum=2**10):
    """ FNV-1a Hash Function. """
    fnv_prime = 16777619
    offset_basis = 2166136261
    h = offset_basis + seed
    for char in text:
        h = h ^ ord(char)
        h = h * fnv_prime
    return h % maximum


def contains(begin: int, end: int, node: int) -> bool:
    """Check node is contained between begin and end in a ring."""
    if end < begin:
        return node > begin or node <= end
    return begin < node <= end
