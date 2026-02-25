from storage.state import get_state, add_insight, add_alert, get_metric_history
from datetime import datetime


def _get_disk_pct_and_gb(metrics):
    disk_metric = metrics.get("disk_used", {})
    disk_val = disk_metric.get("value", 0)
    disk_unit = (disk_metric.get("unit", "") or "").lower()
    is_pct = "percent" in disk_unit or (0 <= disk_val <= 100 and disk_unit != "bytes")
    if is_pct:
        storage_limit_gb = 5
        cluster_info = get_state().get("cluster_info", {})
        if cluster_info:
            storage_limit_gb = cluster_info.get("storage_per_broker_gb", 5)
        disk_gb = (disk_val / 100.0) * storage_limit_gb
        disk_pct = disk_val
    else:
        disk_gb = disk_val / (1024 ** 3)
        disk_pct = None
    return disk_val, disk_gb, disk_pct, is_pct


def _format_disk(disk_val, disk_gb, disk_pct, is_pct):
    if is_pct:
        return f"{disk_pct:.2f}% ({disk_gb:.2f} GB)"
    return f"{disk_gb:.1f} GB"


def analyze_cluster_health():
    state = get_state()
    cluster = state.get("cluster_info")
    metrics = state.get("broker_metrics", {})
    health_score = 100
    issues = []
    recommendations = []

    if not cluster or "error" in str(cluster):
        return {
            "health_score": 0,
            "status": "unknown",
            "issues": ["Unable to connect to cluster"],
            "recommendations": ["Check AWS credentials and cluster ARN configuration"],
        }

    if cluster.get("state") != "ACTIVE":
        health_score -= 50
        issues.append(f"Cluster is in {cluster.get('state')} state")
        recommendations.append("Investigate cluster state and wait for operations to complete")

    if metrics.get("cpu", {}).get("value", 0) > 80:
        health_score -= 20
        issues.append(f"High CPU usage: {metrics['cpu']['value']:.1f}%")
        recommendations.append("Consider scaling broker instance type or adding brokers")
    elif metrics.get("cpu", {}).get("value", 0) > 60:
        health_score -= 10
        issues.append(f"Elevated CPU usage: {metrics['cpu']['value']:.1f}%")
        recommendations.append("Monitor CPU trends; plan for scaling if usage continues to rise")

    disk_val, disk_gb, disk_pct, is_pct = _get_disk_pct_and_gb(metrics)
    disk_display = _format_disk(disk_val, disk_gb, disk_pct, is_pct)
    if is_pct:
        if disk_pct > 80:
            health_score -= 25
            issues.append(f"Critical storage usage: {disk_display}")
            recommendations.append("Immediately expand EBS storage or reduce retention period")
        elif disk_pct > 50:
            health_score -= 10
            issues.append(f"High storage usage: {disk_display}")
            recommendations.append("Plan storage expansion or review log retention settings")
    else:
        if disk_gb > 100:
            health_score -= 25
            issues.append(f"Critical storage usage: {disk_display}")
            recommendations.append("Immediately expand EBS storage or reduce retention period")
        elif disk_gb > 50:
            health_score -= 10
            issues.append(f"High storage usage: {disk_display}")
            recommendations.append("Plan storage expansion or review log retention settings")

    under_replicated = metrics.get("under_replicated", {}).get("value", 0)
    if under_replicated > 0:
        health_score -= 20
        issues.append(f"{int(under_replicated)} under-replicated partitions")
        recommendations.append("Check broker health and network connectivity between brokers")

    offline_parts = metrics.get("offline_partitions", {}).get("value", 0)
    if offline_parts > 0:
        health_score -= 30
        issues.append(f"{int(offline_parts)} offline partitions detected")
        recommendations.append("Critical: Investigate broker failures immediately")

    fetch_latency = metrics.get("fetch_latency", {}).get("value", 0)
    if fetch_latency > 500:
        health_score -= 15
        issues.append(f"Very high fetch latency: {fetch_latency:.1f} ms")
        recommendations.append("Investigate disk I/O contention, reduce partitions per broker, or tune consumer fetch.max.wait.ms and max.partition.fetch.bytes")
    elif fetch_latency > 200:
        health_score -= 5
        issues.append(f"Elevated fetch latency: {fetch_latency:.1f} ms")
        recommendations.append("Monitor fetch latency trends; consider tuning consumer fetch settings or checking broker disk performance")

    health_score = max(0, health_score)

    if health_score >= 90:
        status = "healthy"
    elif health_score >= 70:
        status = "warning"
    elif health_score >= 50:
        status = "degraded"
    else:
        status = "critical"

    result = {
        "health_score": health_score,
        "status": status,
        "issues": issues,
        "recommendations": recommendations,
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }

    from storage.state import update_state
    update_state("health_score", health_score)

    if issues:
        add_insight(
            "health_analysis",
            f"Cluster Health: {status.upper()} ({health_score}/100)",
            f"Found {len(issues)} issue(s) affecting cluster health.",
            recommendations
        )

    return result


def _format_bytes(b):
    if b >= 1073741824:
        return f"{b / 1073741824:.1f} GB"
    elif b >= 1048576:
        return f"{b / 1048576:.1f} MB"
    elif b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b:.0f} B"


def detect_anomalies():
    anomalies = []

    state = get_state()
    topic_metrics = state.get("topic_metrics", {})
    suppressed_topics = state.get("suppressed_topics", [])
    topic_metrics = {k: v for k, v in topic_metrics.items() if k not in suppressed_topics}
    broker_metrics = state.get("broker_metrics", {})

    if topic_metrics and isinstance(topic_metrics, dict) and "error" not in topic_metrics:
        total_bytes_in = sum(max(t.get("bytes_in_per_sec", 0), 0) for t in topic_metrics.values())
        if total_bytes_in < 1:
            total_bytes_in = 0
        _, disk_gb, _, _ = _get_disk_pct_and_gb(broker_metrics)

        top_producers = []
        for topic_name, tdata in topic_metrics.items():
            bytes_in = max(tdata.get("bytes_in_per_sec", 0), 0)
            if bytes_in < 1:
                bytes_in = 0
            msgs_in = tdata.get("messages_in_per_sec", 0)
            pct_share = (bytes_in / total_bytes_in * 100) if total_bytes_in > 0 else 0
            history = tdata.get("bytes_in_history", [])
            avg_bytes = tdata.get("bytes_in_avg", 0)

            is_spiking = False
            spike_factor = 1.0
            if avg_bytes > 0 and bytes_in > 0:
                spike_factor = bytes_in / avg_bytes
                if spike_factor > 1.5:
                    is_spiking = True

            is_dominant = pct_share > 25 and bytes_in > 1
            is_critical_dominant = pct_share > 50 and bytes_in > 1
            is_high_throughput = bytes_in > 500000

            status = "normal"
            if is_critical_dominant:
                status = "critical"
            elif is_dominant or is_high_throughput or is_spiking:
                status = "warning"

            top_producers.append({
                "topic": topic_name,
                "bytes_in_per_sec": bytes_in,
                "messages_in_per_sec": msgs_in,
                "pct_of_total": pct_share,
                "avg_bytes_in": avg_bytes,
                "is_spiking": is_spiking,
                "is_dominant": is_dominant,
                "is_high_throughput": is_high_throughput,
                "spike_factor": spike_factor,
                "status": status,
            })

        top_producers.sort(key=lambda x: x["bytes_in_per_sec"], reverse=True)

        if top_producers:
            anomalies.append({
                "metric": "Top Producers Summary",
                "type": "producer_summary",
                "current_value": f"{len(top_producers)} active topic(s)",
                "average_value": f"Total ingress: {_format_bytes(total_bytes_in)}/s",
                "threshold": "N/A",
                "unit": "",
                "severity": "info",
                "trend": "monitoring",
                "top_producers": [
                    {
                        "topic": p["topic"],
                        "bytes_in": _format_bytes(p["bytes_in_per_sec"]) + "/s",
                        "messages_in": f"{p['messages_in_per_sec']:.1f} msg/s" if p['messages_in_per_sec'] < 10 else f"{p['messages_in_per_sec']:.0f} msg/s",
                        "pct_share": f"{p['pct_of_total']:.1f}%",
                        "spiking": p["is_spiking"],
                        "status": p["status"],
                        "is_dominant": p.get("is_dominant", False),
                        "is_high_throughput": p.get("is_high_throughput", False),
                    }
                    for p in top_producers[:5]
                ],
                "recommended_action": "Monitor producer distribution; consider quotas if any single topic exceeds 50% of total ingress",
            })

            for p in top_producers:
                if p["status"] == "critical":
                    anomalies.append({
                        "metric": f"Dominant Producer: {p['topic']}",
                        "type": "producer_anomaly",
                        "current_value": f"{_format_bytes(p['bytes_in_per_sec'])}/s ({p['pct_of_total']:.1f}% of total ingress)",
                        "average_value": f"Avg: {_format_bytes(p['avg_bytes_in'])}/s" if p["avg_bytes_in"] > 0 else "No baseline",
                        "threshold": ">50% of total ingress + spiking",
                        "unit": "bytes/s",
                        "severity": "critical",
                        "trend": "spiking",
                        "recommended_action": f"Topic '{p['topic']}' is dominating cluster ingress at {p['pct_of_total']:.1f}% and spiking. Apply producer quotas (producer_byte_rate) or throttle the producer immediately.",
                    })
                elif p["status"] == "warning":
                    reason = ""
                    if p.get("is_dominant"):
                        reason = f"dominating {p['pct_of_total']:.1f}% of total ingress"
                    elif p.get("is_high_throughput"):
                        reason = f"high throughput at {_format_bytes(p['bytes_in_per_sec'])}/s"
                    elif p.get("is_spiking"):
                        reason = f"spiking {p['spike_factor']:.1f}x above average"
                    anomalies.append({
                        "metric": f"Producer Warning: {p['topic']}",
                        "type": "producer_anomaly",
                        "current_value": f"{_format_bytes(p['bytes_in_per_sec'])}/s ({p['pct_of_total']:.1f}% of total ingress)",
                        "average_value": f"Avg: {_format_bytes(p['avg_bytes_in'])}/s" if p["avg_bytes_in"] > 0 else "No baseline",
                        "threshold": reason,
                        "unit": "bytes/s",
                        "severity": "warning",
                        "trend": "elevated",
                        "recommended_action": f"Topic '{p['topic']}' is {reason}. Monitor closely and consider applying producer quotas if ingress continues to grow.",
                    })

    return anomalies


def generate_root_cause_analysis():
    state = get_state()
    metrics = state.get("broker_metrics", {})
    cluster = state.get("cluster_info", {})
    alerts = list(state.get("alerts", []))

    if not alerts:
        return {"summary": "No active issues detected.", "probable_causes": [], "recommended_actions": []}

    critical_alerts = [a for a in alerts if a.get("severity") == "critical"]
    warning_alerts = [a for a in alerts if a.get("severity") == "warning"]

    probable_causes = []
    recommended_actions = []

    cpu_high = metrics.get("cpu", {}).get("value", 0) > 70
    _, disk_gb_rca, disk_pct_rca, is_pct_rca = _get_disk_pct_and_gb(metrics)
    disk_high = (disk_pct_rca > 50) if is_pct_rca else (disk_gb_rca > 50)
    under_rep = metrics.get("under_replicated", {}).get("value", 0) > 0

    if disk_high and under_rep:
        probable_causes.append(
            "Under-replicated partitions may be caused by disk pressure preventing log replication"
        )
        recommended_actions.append("Expand EBS storage immediately")
        recommended_actions.append("Review log retention policies to free disk space")

    if under_rep and not cpu_high and not disk_high:
        probable_causes.append(
            "Under-replicated partitions without resource pressure may indicate network issues between brokers"
        )
        recommended_actions.append("Check VPC security group rules and network ACLs")
        recommended_actions.append("Verify broker-to-broker connectivity")

    if not probable_causes:
        for alert in critical_alerts[:3]:
            probable_causes.append(f"Alert: {alert['title']} - {alert['message']}")
        recommended_actions.append("Investigate recent cluster changes and CloudWatch metrics")

    summary = f"Analyzed {len(alerts)} alert(s): {len(critical_alerts)} critical, {len(warning_alerts)} warnings. "
    if probable_causes:
        summary += f"Identified {len(probable_causes)} probable cause(s)."

    return {
        "summary": summary,
        "probable_causes": probable_causes,
        "recommended_actions": recommended_actions,
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }


def get_optimization_recommendations():
    state = get_state()
    cluster = state.get("cluster_info", {})
    metrics = state.get("broker_metrics", {})
    topic_metrics = state.get("topic_metrics", {})
    recommendations = []

    broker_count = cluster.get("broker_count", 0)
    instance_type = cluster.get("instance_type", "")

    top_producers = []
    total_bytes_in = 0
    if topic_metrics and isinstance(topic_metrics, dict) and "error" not in topic_metrics:
        total_bytes_in = sum(t.get("bytes_in_per_sec", 0) for t in topic_metrics.values())
        for topic_name, tdata in topic_metrics.items():
            bytes_in = tdata.get("bytes_in_per_sec", 0)
            pct = (bytes_in / total_bytes_in * 100) if total_bytes_in > 0 else 0
            top_producers.append({
                "topic": topic_name,
                "bytes_in": bytes_in,
                "pct": pct,
                "msgs_in": tdata.get("messages_in_per_sec", 0),
                "partitions": tdata.get("partition_count", 0),
            })
        top_producers.sort(key=lambda x: x["bytes_in"], reverse=True)

    cpu_val = metrics.get("cpu", {}).get("value", 0)
    if cpu_val < 20 and broker_count > 3:
        desc = f"Average CPU is only {cpu_val:.1f}%. The cluster may be over-provisioned."
        if top_producers:
            active = [p for p in top_producers if p["bytes_in"] > 0]
            desc += f" Only {len(active)} of {len(top_producers)} topics are actively producing data."
        recommendations.append({
            "category": "cost",
            "title": "Consider Downsizing Brokers",
            "description": desc,
            "impact": "medium",
            "action": "Review broker instance type and consider downsizing",
        })

    if cpu_val > 70:
        desc = f"CPU usage at {cpu_val:.1f}% indicates brokers are under pressure."
        if top_producers:
            top3 = [f"'{p['topic']}' ({_format_bytes(p['bytes_in'])}/s, {p['pct']:.1f}%)" for p in top_producers[:3] if p["bytes_in"] > 0]
            if top3:
                desc += f" Heaviest producers: {'; '.join(top3)}."
        recommendations.append({
            "category": "performance",
            "title": "Scale Up Broker Instances",
            "description": desc,
            "impact": "high",
            "action": "Upgrade to a larger instance type or add more brokers",
            "affected_topics": [p["topic"] for p in top_producers[:3]],
        })

    _, disk_gb_opt, disk_pct_opt, is_pct_opt = _get_disk_pct_and_gb(metrics)
    disk_display_opt = _format_disk(0, disk_gb_opt, disk_pct_opt, is_pct_opt)
    disk_needs_action = (disk_pct_opt > 50) if is_pct_opt else (disk_gb_opt > 50)
    if disk_needs_action:
        desc = f"Storage used: {disk_display_opt}. Consider expanding."
        if top_producers:
            biggest = top_producers[0]
            if biggest["pct"] > 30:
                desc += f" Topic '{biggest['topic']}' is the largest ingress contributor ({biggest['pct']:.1f}% of total) and likely the primary storage consumer."
        recommendations.append({
            "category": "storage",
            "title": "Expand Broker Storage",
            "description": desc,
            "impact": "high",
            "action": f"Increase EBS volume size from current {cluster.get('storage_per_broker_gb', 'N/A')}GB" + (f". Consider reducing retention on topic '{top_producers[0]['topic']}'" if top_producers and top_producers[0]["pct"] > 30 else ""),
            "affected_topics": [p["topic"] for p in top_producers[:3] if p["pct"] > 10],
        })

    partition_history = get_metric_history("partition_count")
    if len(partition_history) >= 2:
        p_values = [h["value"] for h in partition_history]
        p_new = int(p_values[-1])
        p_old = int(p_values[0])
        if p_new > p_old:
            desc = f"Partition count has increased from {p_old} to {p_new} (+{p_new - p_old})."
            topics_by_partitions = sorted(top_producers, key=lambda x: x["partitions"], reverse=True)
            topics_with_parts = [p for p in topics_by_partitions if p["partitions"] > 0]
            if topics_with_parts:
                part_details = [f"'{p['topic']}' ({p['partitions']} partitions, {_format_bytes(p['bytes_in'])}/s)" for p in topics_with_parts[:5]]
                desc += f" Topics by partition count: {'; '.join(part_details)}."
                low_throughput_high_part = [p for p in topics_with_parts if p["partitions"] > 3 and p["bytes_in"] < 1024]
                if low_throughput_high_part:
                    waste = [f"'{p['topic']}' ({p['partitions']} partitions, near-zero throughput)" for p in low_throughput_high_part[:3]]
                    desc += f" Low-activity topics with high partition counts: {'; '.join(waste)}."
            recommendations.append({
                "category": "performance",
                "title": "Optimize Topic Partition Count",
                "description": desc,
                "impact": "medium" if (p_new - p_old) < 20 else "high",
                "action": "Review partition allocation per topic. Reduce partitions on low-throughput topics to lower metadata overhead and broker memory usage.",
                "affected_topics": [p["topic"] for p in topics_with_parts[:5]],
            })

    monitoring_level = cluster.get("enhanced_monitoring", "DEFAULT")
    if monitoring_level == "DEFAULT":
        recommendations.append({
            "category": "observability",
            "title": "Enable Enhanced Monitoring",
            "description": "Enhanced monitoring is disabled. Enable PER_BROKER level for detailed metrics.",
            "impact": "medium",
            "action": "Update monitoring level to PER_BROKER or PER_TOPIC_PER_BROKER",
        })

    return recommendations


def generate_predictive_analysis():
    state = get_state()
    metrics = state.get("broker_metrics", {})
    cluster = state.get("cluster_info", {})
    predictions = []

    metric_configs = [
        ("cpu", "CPU Usage", "%", 80, "Scale broker instance type"),
        ("disk_used", "Storage Used", "auto", None, "Expand EBS storage"),
        ("throughput_in", "Bytes In/sec", "bytes/s", None, "Add brokers or upgrade instance"),
        ("messages_in", "Messages In/sec", "msg/s", None, "Review partition strategy"),
    ]

    for metric_name, display_name, unit, threshold, action in metric_configs:
        history = get_metric_history(metric_name)
        if len(history) < 2:
            continue

        values = [h["value"] for h in history]
        n = len(values)
        latest = values[-1]
        avg = sum(values) / n
        std_dev = (sum((v - avg) ** 2 for v in values) / n) ** 0.5

        if n >= 3:
            recent_half = values[n // 2:]
            old_half = values[:n // 2]
            recent_avg = sum(recent_half) / len(recent_half) if recent_half else 0
            old_avg = sum(old_half) / len(old_half) if old_half else 0
            change_rate = recent_avg - old_avg
        else:
            change_rate = 0

        if change_rate > 0:
            trend = "increasing"
        else:
            trend = "normal"

        projected_1h = latest + change_rate * 60
        projected_6h = latest + change_rate * 360
        projected_24h = latest + change_rate * 1440

        pred_unit = unit
        pred_threshold = threshold
        if unit == "auto" and metric_name == "disk_used":
            disk_metric = metrics.get("disk_used", {})
            metric_unit_str = (disk_metric.get("unit", "") or "").lower()
            if "percent" in metric_unit_str or (0 <= latest <= 100 and metric_unit_str != "bytes"):
                pred_unit = "%"
                pred_threshold = 80
            else:
                pred_unit = "bytes"
                storage_limit_bytes = cluster.get("storage_per_broker_gb", 100) * (1024 ** 3)
                pred_threshold = int(storage_limit_bytes * 0.9)

        def fmt_val(v):
            if pred_unit == "bytes":
                return _format_bytes(max(0, v))
            elif pred_unit == "bytes/s":
                return _format_bytes(max(0, v)) + "/s"
            elif pred_unit == "%":
                return f"{max(0, v):.2f}%"
            elif pred_unit == "ms":
                return f"{max(0, v):.1f} ms"
            elif pred_unit == "msg/s":
                return f"{max(0, v):.1f} msg/s"
            return f"{max(0, v):.1f}"

        risk = "low"
        time_to_threshold = None
        if pred_threshold and change_rate > 0 and latest < pred_threshold:
            intervals = (pred_threshold - latest) / change_rate
            hours = intervals / 60
            time_to_threshold = f"{hours:.1f} hours" if hours < 48 else f"{hours / 24:.1f} days"
            if hours < 6:
                risk = "critical"
            elif hours < 24:
                risk = "high"
            elif hours < 72:
                risk = "medium"

        volatility = "low"
        if avg > 0:
            cv = std_dev / avg
            if cv > 0.5:
                volatility = "high"
            elif cv > 0.2:
                volatility = "medium"

        predictions.append({
            "metric": display_name,
            "current": fmt_val(latest),
            "average": fmt_val(avg),
            "trend": trend,
            "projected_1h": fmt_val(projected_1h),
            "projected_6h": fmt_val(projected_6h),
            "projected_24h": fmt_val(projected_24h),
            "risk_level": risk,
            "volatility": volatility,
            "time_to_threshold": time_to_threshold,
            "recommended_action": action if risk in ("high", "critical") else None,
            "data_points": n,
        })

    predictions.sort(key=lambda p: {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(p["risk_level"], 4))

    return {
        "predictions": predictions,
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }
