# tests/performance/utils/spark_metrics.py

import requests
import os

SPARK_UI_URL = os.environ.get("SPARK_UI_URL", "http://spark_db:4040")


def get_executor_metrics() -> dict:
    """
    Polls the Spark REST API for live executor resource usage.
    Returns driver + executor memory/CPU summary.

    Spark REST docs:
      GET /api/v1/applications/{appId}/executors
    """
    # First get the active application ID
    apps = requests.get(f"{SPARK_UI_URL}/api/v1/applications", timeout=5).json()
    if not apps:
        return {}

    app_id = apps[0]["id"]

    executors = requests.get(
        f"{SPARK_UI_URL}/api/v1/applications/{app_id}/executors",
        timeout=5
    ).json()

    # The driver shows up as executor id "driver"
    driver = next((e for e in executors if e["id"] == "driver"), {})
    workers = [e for e in executors if e["id"] != "driver"]

    return {
        "driver_heap_used_mb": driver.get("memoryUsed", 0) / (1024 ** 2),
        "driver_heap_max_mb": driver.get("maxMemory", 0) / (1024 ** 2),
        "executor_count": len(workers),
        "executor_heap_used_mb": sum(
            e.get("memoryUsed", 0) for e in workers
        ) / (1024 ** 2),
        "executor_total_tasks": sum(
            e.get("totalTasks", 0) for e in workers
        ),
        "executor_active_tasks": sum(
            e.get("activeTasks", 0) for e in workers
        ),
    }