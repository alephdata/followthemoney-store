
def to_bytes(s):
    if isinstance(s, bytes):
        return s
    return s.encode()
