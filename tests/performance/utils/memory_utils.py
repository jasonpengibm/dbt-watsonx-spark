import gc
import psutil


def profile_memory(operation_fn, label="", backend="standard"):
    """
    Measures OS-level RSS delta around a single operation.

    gc.collect() is called before and after to flush Python's allocator
    page cache, so retained-but-freed pages don't inflate rss_delta.

    Args:
        operation_fn: Zero-argument callable to profile.
        label:        Human-readable description of the operation.
        backend:      Spark execution backend label (e.g. 'standard', 'gluten').
                      Included in the printed output so CSV artifacts carry
                      backend metadata alongside memory measurements.
    """
    gc.collect()
    process = psutil.Process()
    rss_before = process.memory_info().rss

    operation_fn()

    gc.collect()
    rss_after = process.memory_info().rss

    rss_delta = (rss_after - rss_before) / (1024 ** 2)
    print(f"\n[mem][{backend}] {label}: rss_delta={rss_delta:.1f} MB")

    return {
        "rss_before_mb": rss_before / (1024 ** 2),
        "rss_after_mb": rss_after / (1024 ** 2),
        "rss_delta_mb": rss_delta,
    }
