"""Shared test setup.

The library itself never reads ``.env`` -- that is the caller's job -- so the
integration tests load it here. Tests still run without a key, just against the
lower unauthenticated NCBI rate limit.
"""

from dotenv import load_dotenv

load_dotenv()
