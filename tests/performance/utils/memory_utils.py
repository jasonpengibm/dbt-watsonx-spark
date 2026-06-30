import gc

import psutil

"""
Measures OS-level RSS delta around a single operation.

Returns
rss_before_mb  -- RSS before the operation
rss_after_mb   -- RSS after operation
rss_delta_mb   -- net change in MB 
"""
def profile_memory(operation_fn, label=""):

    gc.collect()
    process = psutil.Process()
    rss_before = process.memory_info().rss

    operation_fn()

    gc.collect()
    rss_after = process.memory_info().rss

    rss_delta = (rss_after - rss_before) / (1024 ** 2)
    print(f"\n[mem] {label}: rss_delta={rss_delta:.1f} MB")

    return {
        "rss_before_mb": rss_before / (1024 ** 2),
        "rss_after_mb": rss_after / (1024 ** 2),
        "rss_delta_mb": rss_delta,
    }
