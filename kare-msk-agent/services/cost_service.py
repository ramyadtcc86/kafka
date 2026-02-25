from storage.state import get_state, get_metric_history, add_insight
from datetime import datetime

INSTANCE_COSTS = {
    "kafka.t3.small": 0.067,
    "kafka.m5.large": 0.21,
    "kafka.m5.xlarge": 0.42,
    "kafka.m5.2xlarge": 0.84,
    "kafka.m5.4xlarge": 1.68,
    "kafka.m5.8xlarge": 3.36,
    "kafka.m5.12xlarge": 5.04,
    "kafka.m5.16xlarge": 6.72,
    "kafka.m5.24xlarge": 10.08,
    "kafka.m7g.large": 0.204,
    "kafka.m7g.xlarge": 0.408,
    "kafka.m7g.2xlarge": 0.816,
    "express.m7g.large": 0.408,
    "express.m7g.4xlarge": 1.632,
    "express.m7g.16xlarge": 6.528,
}

STORAGE_COST_PER_GB_MONTH = 0.10
EXPRESS_DATA_INGESTION_PER_GB = 0.01


def _is_express_broker(instance_type):
    return instance_type.startswith("express.")


def _format_bytes_short(b):
    if b >= 1073741824:
        return f"{b / 1073741824:.1f} GB"
    elif b >= 1048576:
        return f"{b / 1048576:.1f} MB"
    elif b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b:.0f} B"


def estimate_monthly_cost():
    state = get_state()
    cluster = state.get("cluster_info", {})
    metrics = state.get("broker_metrics", {})

    if not cluster or "error" in str(cluster):
        return {"error": "Cluster info not available"}

    instance_type = cluster.get("instance_type", "")
    broker_count = cluster.get("broker_count", 0)
    is_express = _is_express_broker(instance_type)

    hourly_rate = INSTANCE_COSTS.get(instance_type, 0.21)
    compute_monthly = hourly_rate * 730 * broker_count

    disk_bytes = metrics.get("disk_used", {}).get("value", 0)
    current_storage_gb = disk_bytes / (1024 ** 3)

    if is_express:
        storage_gb_for_cost = current_storage_gb * broker_count
        storage_monthly = storage_gb_for_cost * STORAGE_COST_PER_GB_MONTH

        throughput_in_metric = metrics.get("throughput_in", {})
        throughput_in_avg = throughput_in_metric.get("value", 0)
        total_throughput_in = throughput_in_avg * broker_count
        monthly_ingestion_gb = (total_throughput_in * 730 * 3600) / (1024 ** 3) if total_throughput_in > 0 else 0
        data_ingestion_monthly = monthly_ingestion_gb * EXPRESS_DATA_INGESTION_PER_GB

        total_monthly = compute_monthly + storage_monthly + data_ingestion_monthly
        return {
            "instance_type": instance_type,
            "broker_type": "Express",
            "broker_count": broker_count,
            "storage_type": "Elastic (pay-as-you-go)",
            "current_storage_used": _format_bytes_short(disk_bytes) if disk_bytes > 0 else "N/A",
            "current_storage_gb": round(current_storage_gb, 2),
            "compute_cost_monthly": round(compute_monthly, 2),
            "storage_cost_monthly": round(storage_monthly, 2),
            "data_ingestion_cost_monthly": round(data_ingestion_monthly, 2),
            "estimated_monthly_ingestion_gb": round(monthly_ingestion_gb, 1),
            "total_estimated_monthly": round(total_monthly, 2),
            "hourly_rate_per_broker": hourly_rate,
        }
    else:
        storage_per_broker = cluster.get("storage_per_broker_gb", 0)
        storage_monthly = storage_per_broker * broker_count * STORAGE_COST_PER_GB_MONTH
        total_monthly = compute_monthly + storage_monthly
        return {
            "instance_type": instance_type,
            "broker_type": "Standard",
            "broker_count": broker_count,
            "storage_type": "Provisioned",
            "storage_per_broker_gb": storage_per_broker,
            "current_storage_used": _format_bytes_short(disk_bytes),
            "current_storage_gb": round(current_storage_gb, 2),
            "compute_cost_monthly": round(compute_monthly, 2),
            "storage_cost_monthly": round(storage_monthly, 2),
            "total_estimated_monthly": round(total_monthly, 2),
            "hourly_rate_per_broker": hourly_rate,
        }


def get_cost_recommendations():
    state = get_state()
    cluster = state.get("cluster_info", {})
    metrics = state.get("broker_metrics", {})
    recommendations = []

    cpu_val = metrics.get("cpu", {}).get("value", 0)
    instance_type = cluster.get("instance_type", "")
    broker_count = cluster.get("broker_count", 0)
    is_express = _is_express_broker(instance_type)

    if cpu_val > 0 and cpu_val < 15 and broker_count > 3:
        recommendations.append({
            "type": "downsize_brokers",
            "title": "Reduce Broker Count",
            "description": f"With only {cpu_val:.1f}% CPU usage across {broker_count} brokers, you may be over-provisioned.",
            "estimated_savings": f"~${INSTANCE_COSTS.get(instance_type, 0.21) * 730:.0f}/month per broker removed",
            "risk": "medium",
        })

    if cpu_val > 0 and cpu_val < 10 and not is_express:
        sorted_types = sorted(INSTANCE_COSTS.items(), key=lambda x: x[1])
        current_cost = INSTANCE_COSTS.get(instance_type, 0)
        cheaper = [(t, c) for t, c in sorted_types if c < current_cost and not t.startswith("express.")]
        if cheaper:
            suggested_type, suggested_cost = cheaper[-1]
            savings = (current_cost - suggested_cost) * 730 * broker_count
            recommendations.append({
                "type": "downsize_instance",
                "title": "Downsize Instance Type",
                "description": f"Current {instance_type} is underutilized. Consider {suggested_type}.",
                "estimated_savings": f"~${savings:.0f}/month",
                "risk": "medium",
            })

    disk_bytes = metrics.get("disk_used", {}).get("value", 0)
    disk_gb = disk_bytes / (1024 ** 3)

    if not is_express:
        storage_gb = cluster.get("storage_per_broker_gb", 0)
        if storage_gb > 0 and disk_gb < (storage_gb * 0.3) and storage_gb > 100:
            recommendations.append({
                "type": "optimize_storage",
                "title": "Review Storage Allocation",
                "description": f"Only {disk_gb:.1f} GB of {storage_gb} GB provisioned storage is used. Consider reducing retention.",
                "estimated_savings": "Varies based on retention reduction",
                "risk": "low",
            })

    if is_express:
        throughput_in_avg = metrics.get("throughput_in", {}).get("value", 0)
        total_throughput_in = throughput_in_avg * broker_count
        if total_throughput_in > 0:
            monthly_ingestion_gb = (total_throughput_in * 730 * 3600) / (1024 ** 3)
            ingestion_cost = monthly_ingestion_gb * EXPRESS_DATA_INGESTION_PER_GB
            if ingestion_cost > 50:
                recommendations.append({
                    "type": "optimize_ingestion",
                    "title": "Review Data Ingestion Rate",
                    "description": f"Estimated {monthly_ingestion_gb:.0f} GB/month ingestion costs ~${ingestion_cost:.0f}/month. Consider compression or reducing throughput.",
                    "estimated_savings": "Up to 50% with producer compression enabled",
                    "risk": "low",
                })

    cost_data = estimate_monthly_cost()
    from storage.state import update_state
    update_state("cost_insights", {
        "estimated_cost": cost_data,
        "recommendations": recommendations,
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    })

    return {
        "estimated_cost": cost_data,
        "recommendations": recommendations,
    }
