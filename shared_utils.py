import hashlib
import time

# Cache tracking to be used by both modules
prefetched_contexts = {}
CACHE_TTL_MEDIUM = 3600  # 1 hour cache expiration

def get_cache_key(source_level_url):
    """Generate a cache key for prefetched context"""
    key_data = f"prefetch:{source_level_url}"
    return hashlib.md5(key_data.encode()).hexdigest()