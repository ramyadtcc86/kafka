from storage.state import get_state, get_metric_history, update_state, add_alert
from datetime import datetime, timedelta


def _format_bytes_short(b):
    if b >= 1073741824:
        return f"{b / 1073741824:.1f} GB"
    elif b >= 1048576:
        return f"{b / 1048576:.1f} MB"
    elif b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b:.0f} B"


def forecast_storage():
    state = get_state()
    cluster = state.get("cluster_info", {})
    metrics = state.get("broker_metrics", {})

    if not cluster:
        cluster = {}
    if not metrics:
        metrics = {}

    disk_metric = metrics.get("disk_used", {})
    disk_val = disk_metric.get("value", 0)
    metric_unit = (disk_metric.get("unit", "") or "").lower()
    is_pct = "percent" in metric_unit or (0 <= disk_val <= 100 and metric_unit != "bytes")

    storage_limit_gb = cluster.get("storage_per_broker_gb", 0)
    instance_type = cluster.get("instance_type", "")
    is_express = instance_type.startswith("express.")

    if is_pct:
        usage_pct = disk_val
        used_gb = (disk_val / 100.0) * storage_limit_gb if storage_limit_gb > 0 else 0
    else:
        used_gb = disk_val / (1024 ** 3)
        usage_pct = (used_gb / storage_limit_gb * 100) if storage_limit_gb > 0 else 0

    if storage_limit_gb == 0:
        if is_express:
            storage_limit_gb = max(100, used_gb * 5)
        else:
            storage_limit_gb = max(100, used_gb * 2)
        if is_pct and storage_limit_gb == 0:
            storage_limit_gb = 100

    history = get_metric_history("disk_used")
    daily_growth_pct = 0
    daily_growth_gb = 0
    if len(history) >= 2:
        values = [h["value"] for h in history]
        changes = [values[i] - values[i-1] for i in range(1, len(values))]
        avg_change_per_interval = sum(changes) / len(changes) if changes else 0
        if is_pct:
            daily_growth_pct = avg_change_per_interval * 24 * 60
            daily_growth_gb = (daily_growth_pct / 100.0) * storage_limit_gb
        else:
            daily_growth_bytes = avg_change_per_interval * 24
            daily_growth_gb = daily_growth_bytes / (1024 ** 3)

    days_to_90 = None
    exhaustion_date = None
    if daily_growth_gb > 0:
        remaining_gb = storage_limit_gb * 0.9 - used_gb
        days_to_90 = remaining_gb / daily_growth_gb if daily_growth_gb > 0 else 999
        days_to_90 = min(days_to_90, 365 * 100)
        try:
            exhaustion_date = datetime.utcnow() + timedelta(days=max(0, days_to_90))
        except (OverflowError, ValueError):
            days_to_90 = None
            exhaustion_date = None

        if days_to_90 is not None:
            if days_to_90 < 7:
                add_alert("critical", "Storage Exhaustion Imminent",
                           f"At current growth rate, storage will reach 90% in {days_to_90:.0f} days")
            elif days_to_90 < 30:
                add_alert("warning", "Storage Growth Warning",
                           f"Storage projected to reach 90% in {days_to_90:.0f} days")

    if is_pct:
        used_display = f"{usage_pct:.2f}% ({used_gb:.2f} GB)"
        growth_display = f"{daily_growth_pct:.4f}%/day ({daily_growth_gb:.4f} GB/day)" if daily_growth_gb > 0 else "stable"
    else:
        used_display = _format_bytes_short(disk_val)
        growth_display = _format_bytes_short(daily_growth_gb * (1024 ** 3)) + "/day" if daily_growth_gb > 0 else "stable"

    forecast = {
        "current_usage_pct": round(usage_pct, 2),
        "used_gb": round(used_gb, 2),
        "used_display": used_display,
        "free_gb": round(storage_limit_gb - used_gb, 2),
        "total_storage_gb": storage_limit_gb,
        "storage_type": "Elastic" if is_express else "Provisioned",
        "daily_growth_rate": growth_display,
        "daily_growth_rate_pct": round(daily_growth_gb / storage_limit_gb * 100, 4) if storage_limit_gb > 0 else 0,
        "days_to_90_pct": round(days_to_90, 0) if days_to_90 else None,
        "projected_90_pct_date": exhaustion_date.isoformat() if exhaustion_date else None,
        "recommendation": _storage_recommendation(used_gb, days_to_90, storage_limit_gb, is_express),
    }

    update_state("capacity_forecast", forecast)
    return forecast


def _storage_recommendation(used_gb, days_to_90, limit_gb, is_express=False):
    if is_express:
        if days_to_90 and days_to_90 < 7:
            return "High growth rate detected. Review data retention policies to control storage costs"
        elif days_to_90 and days_to_90 < 30:
            return "Storage growing steadily. Monitor ingestion rates and retention settings"
        elif used_gb > 100:
            return f"Using {used_gb:.1f} GB with elastic storage. Review topic retention to optimize costs"
        else:
            return f"Storage healthy ({used_gb:.1f} GB used). Elastic storage scales automatically"
    usage_pct = (used_gb / limit_gb * 100) if limit_gb > 0 else 0
    if usage_pct > 85:
        return "CRITICAL: Expand storage immediately to prevent data loss"
    elif days_to_90 and days_to_90 < 7:
        return "URGENT: Storage will fill within a week. Expand storage or reduce retention"
    elif days_to_90 and days_to_90 < 30:
        return "Plan storage expansion within the next 2-3 weeks"
    elif usage_pct > 60:
        return "Monitor storage growth. Consider planning expansion"
    else:
        return f"Storage utilization is healthy ({used_gb:.1f} GB used)"


def forecast_throughput():
    state = get_state()
    metrics = state.get("broker_metrics", {})
    cluster = state.get("cluster_info", {})

    throughput_in = metrics.get("throughput_in", {}).get("value", 0)
    throughput_out = metrics.get("throughput_out", {}).get("value", 0)
    broker_count = cluster.get("broker_count", 0)

    per_broker_in = throughput_in / broker_count if broker_count > 0 else 0
    per_broker_out = throughput_out / broker_count if broker_count > 0 else 0

    return {
        "total_throughput_in_bytes_sec": throughput_in,
        "total_throughput_out_bytes_sec": throughput_out,
        "per_broker_in_bytes_sec": round(per_broker_in, 2),
        "per_broker_out_bytes_sec": round(per_broker_out, 2),
        "broker_count": broker_count,
    }


def get_capacity_summary():
    storage = forecast_storage()
    throughput = forecast_throughput()

    return {
        "storage": storage,
        "throughput": throughput,
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }
