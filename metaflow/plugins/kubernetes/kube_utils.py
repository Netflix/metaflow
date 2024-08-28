from hashlib import sha256


def hashed_label(text: str):
    """
    Hash a name for use as a Kubernetes label.
    Use the maximum allowed 63 characters for the hash to minimize collisions.
    """
    return sha256(text.encode("utf-8")).hexdigest()[:63]
