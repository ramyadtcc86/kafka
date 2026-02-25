from services import msk_service
from storage.state import add_remediation_log, add_alert, get_state, get_metric_history
from datetime import datetime


def execute_remediation(action, params=None):
    params = params or {}

    actions = {
        "scale_brokers": _scale_brokers,
        "expand_storage": _expand_storage,
        "enable_monitoring": _enable_monitoring,
        "rebalance_partitions": _rebalance_partitions,
    }

    if action not in actions:
        return {"error": f"Unknown remediation action: {action}"}

    return actions[action](params)


def _scale_brokers(params):
    target = params.get("target_count")
    if not target or not isinstance(target, int):
        return {"error": "target_count must be a positive integer"}

    state = get_state()
    cluster = state.get("cluster_info", {})
    current = cluster.get("broker_count", 0)
    az_count = cluster.get("az_count", 0)

    if target <= current:
        return {"error": f"Target ({target}) must be greater than current broker count ({current})"}

    if target > current * 2:
        return {"error": f"Cannot more than double broker count in one operation. Current: {current}"}

    if az_count > 0 and target % az_count != 0:
        valid_options = [az_count * i for i in range(1, 10) if az_count * i > current][:5]
        return {"error": f"Target broker count ({target}) must be a multiple of the number of Availability Zones ({az_count}). Valid options: {valid_options}"}

    result = msk_service.scale_brokers(target)
    return result


def _expand_storage(params):
    target_gb = params.get("target_size_gb")
    if not target_gb or not isinstance(target_gb, int):
        return {"error": "target_size_gb must be a positive integer"}

    state = get_state()
    cluster = state.get("cluster_info", {})
    current_gb = cluster.get("storage_per_broker_gb", 0)

    if target_gb <= current_gb:
        return {"error": f"Target ({target_gb}GB) must be greater than current storage ({current_gb}GB)"}

    result = msk_service.update_broker_storage(target_gb)
    return result


def _enable_monitoring(params):
    level = params.get("level", "PER_BROKER")
    valid_levels = ["DEFAULT", "PER_BROKER", "PER_TOPIC_PER_BROKER", "PER_TOPIC_PER_PARTITION"]
    if level not in valid_levels:
        return {"error": f"Invalid monitoring level. Must be one of: {valid_levels}"}

    result = msk_service.update_monitoring_level(level)
    return result


def _rebalance_partitions(params):
    result = msk_service.rebalance_partitions()
    return result


def get_available_actions():
    state = get_state()
    cluster = state.get("cluster_info", {})
    metrics = state.get("broker_metrics", {})

    actions = []

    az_count = cluster.get("az_count", 0)
    current_brokers = cluster.get("broker_count", 0)
    valid_targets = [az_count * i for i in range(1, 10) if az_count * i > current_brokers][:5] if az_count > 0 else []
    az_note = f" (must be multiple of {az_count} AZs, e.g. {', '.join(map(str, valid_targets))})" if valid_targets else ""

    actions.append({
        "id": "scale_brokers",
        "name": "Scale Broker Count",
        "description": f"Add more broker nodes to the cluster{az_note}",
        "params": [{"name": "target_count", "type": "integer", "description": f"Target number of brokers{az_note}"}],
        "risk": "medium",
        "current_value": cluster.get("broker_count", "N/A"),
    })

    actions.append({
        "id": "expand_storage",
        "name": "Expand Broker Storage",
        "description": "Increase EBS volume size for all brokers",
        "params": [{"name": "target_size_gb", "type": "integer", "description": "Target storage size in GB"}],
        "risk": "low",
        "current_value": f"{cluster.get('storage_per_broker_gb', 'N/A')}GB",
    })

    actions.append({
        "id": "enable_monitoring",
        "name": "Update Monitoring Level",
        "description": "Change enhanced monitoring level",
        "params": [{"name": "level", "type": "string", "description": "DEFAULT, PER_BROKER, PER_TOPIC_PER_BROKER, or PER_TOPIC_PER_PARTITION"}],
        "risk": "low",
        "current_value": cluster.get("enhanced_monitoring", "N/A"),
    })

    partition_count = metrics.get("partition_count", {}).get("value", "N/A")
    actions.append({
        "id": "rebalance_partitions",
        "name": "Rebalance Partitions",
        "description": "Rebalance partitions across brokers to reduce partition count per broker and alleviate disk I/O contention. Triggers a rolling broker restart.",
        "params": [],
        "risk": "medium",
        "current_value": f"{partition_count} partitions",
    })

    return actions


def _format_bytes(b):
    if b >= 1073741824:
        return f"{b / 1073741824:.1f} GB"
    elif b >= 1048576:
        return f"{b / 1048576:.1f} MB"
    elif b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b:.0f} B"


def _compute_valid_scale_target(broker_count, az_count):
    if az_count <= 0:
        return broker_count + 3
    for multiplier in range(1, 20):
        candidate = az_count * multiplier
        if candidate > broker_count and candidate <= broker_count * 2:
            return candidate
    return None


def _get_top_topic_producers(topic_metrics, top_n=5):
    if not topic_metrics or not isinstance(topic_metrics, dict) or "error" in topic_metrics:
        return [], 0

    total_bytes_in = sum(max(t.get("bytes_in_per_sec", 0), 0) for t in topic_metrics.values())
    if total_bytes_in < 1:
        total_bytes_in = 0
    producers = []
    for topic_name, tdata in topic_metrics.items():
        bytes_in = max(tdata.get("bytes_in_per_sec", 0), 0)
        if bytes_in < 1:
            bytes_in = 0
        msgs_in = tdata.get("messages_in_per_sec", 0)
        pct_share = (bytes_in / total_bytes_in * 100) if total_bytes_in > 0 else 0
        avg_bytes = tdata.get("bytes_in_avg", 0)
        partition_count = tdata.get("partition_count", 0)

        spike_factor = 1.0
        is_spiking = False
        if avg_bytes > 0 and bytes_in > 0:
            spike_factor = bytes_in / avg_bytes
            if spike_factor > 1.5:
                is_spiking = True

        producers.append({
            "topic": topic_name,
            "bytes_in_per_sec": bytes_in,
            "messages_in_per_sec": msgs_in,
            "pct_of_total": pct_share,
            "avg_bytes_in": avg_bytes,
            "is_spiking": is_spiking,
            "spike_factor": spike_factor,
            "partition_count": partition_count,
        })

    producers.sort(key=lambda x: x["bytes_in_per_sec"], reverse=True)
    return producers[:top_n], total_bytes_in


def _build_topic_impact_description(topic_metrics):
    producers, total_bytes = _get_top_topic_producers(topic_metrics, top_n=3)
    if not producers:
        return ""

    parts = []
    for p in producers:
        if p["bytes_in_per_sec"] > 0:
            line = f"Topic '{p['topic']}': {_format_bytes(p['bytes_in_per_sec'])}/s ({p['pct_of_total']:.1f}% of ingress, {p['messages_in_per_sec']:.0f} msg/s"
            if p["partition_count"] > 0:
                line += f", {p['partition_count']} partitions"
            line += ")"
            if p["is_spiking"]:
                line += f" [SPIKE: {p['spike_factor']:.1f}x above avg]"
            parts.append(line)

    if parts:
        return " | Top producers: " + " ; ".join(parts)
    return ""


def get_ai_recommended_actions():
    state = get_state()
    cluster = state.get("cluster_info", {})
    metrics = state.get("broker_metrics", {})
    ai_predictions = state.get("ai_predictions", {})
    topic_metrics = state.get("topic_metrics", {})
    recommendations = []

    broker_count = cluster.get("broker_count", 0)

    disk_history = get_metric_history("disk_used")
    throughput_history = get_metric_history("throughput_in")
    messages_history = get_metric_history("messages_in")
    partition_history = get_metric_history("partition_count")

    top_producers, total_bytes_in = _get_top_topic_producers(topic_metrics)

    cpu_val = metrics.get("cpu", {}).get("value", 0)

    storage_growth_rate = 0
    storage_growing = False
    if len(disk_history) >= 5:
        recent_values = [h["value"] for h in disk_history[-10:]]
        older_values = [h["value"] for h in disk_history[:len(disk_history)//2]] if len(disk_history) >= 10 else [h["value"] for h in disk_history[:3]]
        recent_avg = sum(recent_values) / len(recent_values)
        older_avg = sum(older_values) / len(older_values)
        storage_growth_rate = recent_avg - older_avg
        storage_growing = storage_growth_rate > 1048576

    throughput_spiking = False
    if len(throughput_history) >= 3:
        t_values = [h["value"] for h in throughput_history]
        throughput_avg = sum(t_values) / len(t_values)
        throughput_latest = t_values[-1]
        std_dev = (sum((v - throughput_avg) ** 2 for v in t_values) / len(t_values)) ** 0.5
        if std_dev > 0 and throughput_latest > throughput_avg + 1.5 * std_dev:
            throughput_spiking = True

    messages_spiking = False
    if len(messages_history) >= 3:
        m_values = [h["value"] for h in messages_history]
        messages_avg = sum(m_values) / len(m_values)
        messages_latest = m_values[-1]
        m_std = (sum((v - messages_avg) ** 2 for v in m_values) / len(m_values)) ** 0.5
        if m_std > 0 and messages_latest > messages_avg + 1.5 * m_std:
            messages_spiking = True

    partition_increased = False
    partition_old = 0
    partition_new = 0
    if len(partition_history) >= 2:
        partition_values = [h["value"] for h in partition_history]
        partition_new = int(partition_values[-1])
        partition_old = int(partition_values[0])
        if partition_new > partition_old:
            partition_increased = True

    def _is_duplicate_of_existing(new_title, new_desc, existing_recs):
        new_text = (new_title + " " + new_desc).lower()
        new_keys = set()
        for phrase, key in [
            ("fetch latency", "fetch_latency"), ("fetch", "fetch_latency"),
            ("latency", "fetch_latency"), ("consumer lag", "consumer_lag"),
            ("throughput", "throughput"), ("bytes in", "throughput"),
            ("storage", "storage"), ("disk", "storage"),
            ("cpu", "cpu"), ("partition", "partition"),
            ("producer", "producer"), ("scale", "scale"),
        ]:
            if phrase in new_text:
                new_keys.add(key)
        if not new_keys:
            return False
        for existing in existing_recs:
            existing_text = (existing.get("title", "") + " " + existing.get("description", "")).lower()
            existing_keys = set()
            for phrase, key in [
                ("fetch latency", "fetch_latency"), ("fetch", "fetch_latency"),
                ("latency", "fetch_latency"), ("consumer lag", "consumer_lag"),
                ("throughput", "throughput"), ("bytes in", "throughput"),
                ("storage", "storage"), ("disk", "storage"),
                ("cpu", "cpu"), ("partition", "partition"),
                ("producer", "producer"), ("scale", "scale"),
            ]:
                if phrase in existing_text:
                    existing_keys.add(key)
            overlap = new_keys & existing_keys
            if len(overlap) >= 1 and overlap == new_keys:
                return True
            if len(overlap) >= 2:
                return True
        return False

    if ai_predictions and isinstance(ai_predictions, dict):
        ai_recs = ai_predictions.get("recommendations", [])
        ai_anomalies = ai_predictions.get("anomalies", [])
        fetch_latency_analysis = ai_predictions.get("fetch_latency_analysis", {})
        analyzed_at = ai_predictions.get("analyzed_at", datetime.utcnow().isoformat() + "Z")

        if fetch_latency_analysis and fetch_latency_analysis.get("is_elevated"):
            primary_cause = fetch_latency_analysis.get("primary_cause", "Unknown")
            evidence = fetch_latency_analysis.get("evidence", "")
            root_causes = fetch_latency_analysis.get("root_causes", [])
            recommended_fix = fetch_latency_analysis.get("recommended_fix", "Investigate fetch latency")
            current_ms = fetch_latency_analysis.get("current_ms", "N/A")

            cause_list = ""
            if root_causes:
                cause_list = " Possible causes (ranked): " + "; ".join(str(c) for c in root_causes[:5]) + "."

            desc = f"Fetch latency is elevated at {current_ms}. Primary cause: {primary_cause}."
            if evidence:
                desc += f" Evidence: {evidence}."
            desc += cause_list

            recommendations.append({
                "id": "ai_fetch_latency_rca",
                "type": "ai_insight",
                "severity": "high",
                "title": "High Fetch Latency - Root Cause Analysis",
                "description": desc,
                "recommended_action": recommended_fix,
                "action_type": "info",
                "impact": "Elevated fetch latency increases consumer read times and can cascade into processing delays",
                "source": "Bedrock AI (Llama3 70B)",
                "affected_topics": [],
                "detected_at": analyzed_at,
            })
        elif fetch_latency_analysis and not fetch_latency_analysis.get("is_elevated"):
            current_ms = fetch_latency_analysis.get("current_ms", "N/A")
            primary_cause = fetch_latency_analysis.get("primary_cause", "")
            recommendations.append({
                "id": "ai_fetch_latency_ok",
                "type": "ai_insight",
                "severity": "low",
                "title": "Fetch Latency - Normal",
                "description": f"Fetch latency is at {current_ms} (within normal range). {primary_cause}",
                "recommended_action": "No action needed. Continue monitoring.",
                "action_type": "info",
                "impact": "Fetch latency is healthy",
                "source": "Bedrock AI (Llama3 70B)",
                "affected_topics": [],
                "detected_at": analyzed_at,
            })

        for rec in ai_recs:
            priority = rec.get("priority", "low")
            rec_title = rec.get("title", "AI Recommendation")
            rec_desc = rec.get("description", "")

            if _is_duplicate_of_existing(rec_title, rec_desc, recommendations):
                continue

            rec_topics = []
            for a in ai_anomalies:
                if a.get("topic"):
                    rec_topics.append(a["topic"])
            if not rec_topics:
                rec_topics = [p["topic"] for p in top_producers[:3] if p["bytes_in_per_sec"] > 0]

            recommendations.append({
                "id": f"ai_bedrock_{len(recommendations)}",
                "type": "ai_insight",
                "severity": priority,
                "title": rec_title,
                "description": rec_desc,
                "recommended_action": rec.get("action", "Review and take appropriate action"),
                "action_type": "info",
                "impact": "Based on Bedrock AI analysis of cluster metrics and trends",
                "source": "Bedrock AI (Llama3 70B)",
                "affected_topics": rec_topics if rec_topics else [],
                "detected_at": analyzed_at,
            })

        for anomaly in ai_anomalies:
            severity_map = {"critical": "critical", "warning": "high", "info": "medium"}
            anomaly_severity = severity_map.get(anomaly.get("severity", "info"), "medium")
            anomaly_topic = anomaly.get("topic")
            throttle_action = anomaly.get("throttle_action")
            anomaly_desc = anomaly.get("description", "")
            anomaly_metric = anomaly.get("metric", "Unknown Metric")

            if not throttle_action and anomaly_severity in ("medium",):
                continue

            title = f"AI Anomaly: {anomaly_metric}"
            if anomaly_topic:
                title += f" - Topic '{anomaly_topic}'"

            if _is_duplicate_of_existing(title, anomaly_desc, recommendations):
                continue

            action_text = throttle_action if throttle_action else f"Investigate {anomaly_metric} anomaly and review related metrics"
            affected = [anomaly_topic] if anomaly_topic else []

            recommendations.append({
                "id": f"ai_anomaly_{len(recommendations)}",
                "type": "ai_insight",
                "severity": anomaly_severity,
                "title": title,
                "description": anomaly_desc,
                "recommended_action": action_text,
                "action_type": "info",
                "impact": f"AI-detected anomaly in {anomaly_metric} requires attention",
                "source": "Bedrock AI (Llama3 70B)",
                "affected_topics": affected,
                "detected_at": analyzed_at,
            })

    if not recommendations and not ai_predictions:
        recommendations.append({
            "id": "ai_no_analysis",
            "type": "ai_insight",
            "severity": "low",
            "title": "Run AI Analysis for Deeper Insights",
            "description": "No Bedrock AI analysis has been run yet. Click 'Run AI Analysis' on the AI Insights tab to get intelligent recommendations based on your cluster's current metrics, producer patterns, and capacity trends.",
            "recommended_action": "Navigate to the AI Insights tab and click 'Run AI Analysis' to generate Bedrock-powered recommendations.",
            "action_type": "info",
            "impact": "AI analysis provides predictive insights that go beyond threshold-based alerts",
            "source": "System",
            "detected_at": datetime.utcnow().isoformat() + "Z",
        })

    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    recommendations.sort(key=lambda r: severity_order.get(r.get("severity", "low"), 4))

    ai_assessment = ""
    ai_analyzed_at = ""
    if ai_predictions and isinstance(ai_predictions, dict):
        ai_assessment = ai_predictions.get("overall_assessment", "")
        ai_analyzed_at = ai_predictions.get("analyzed_at", "")

    return {
        "recommendations": recommendations,
        "ai_assessment": ai_assessment,
        "ai_analyzed_at": ai_analyzed_at,
        "analysis_summary": {
            "storage_trend": "growing" if storage_growing else "stable",
            "storage_growth_rate": _format_bytes(abs(storage_growth_rate)) + "/min" if storage_growing else "stable",
            "throughput_status": "spiking" if throughput_spiking else "normal",
            "messages_status": "spiking" if messages_spiking else "normal",
            "cpu_pressure": "high" if cpu_val > 70 else "moderate" if cpu_val > 50 else "low",
            "partition_trend": f"increasing ({partition_old} → {partition_new})" if partition_increased else "stable",
            "total_recommendations": len(recommendations),
        },
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }
