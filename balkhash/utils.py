from hashlib import sha1
from normality import stringify


def safe_fragment(fragment):
    """Make a hashed fragement."""
    fragment = stringify(fragment)
    if fragment is not None:
        fragment = fragment.encode('utf-8', errors='replace')
        return sha1(fragment).hexdigest()
