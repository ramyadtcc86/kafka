from services.aws_client import get_client
from storage.state import get_state, get_metric_history, update_state
from datetime import datetime
import json
import re
import threading


def _parse_ai_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1 or end <= start:
        return None

    cleaned = text[start:end]
    cleaned = re.sub(r'//.*?$', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)
    cleaned = re.sub(r"'([^']*)'", r'"\1"', cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    cleaned2 = re.sub(r'([{,])\s*(\w+)\s*:', r'\1 "\2":', cleaned)
    try:
        return json.loads(cleaned2)
    except json.JSONDecodeError:
        return None

BEDROCK_MODEL_ID = "meta.llama3-70b-instruct-v1:0"

_cache = {
    "last_analysis": None,
    "last_updated": None,
    "lock": threading.Lock(),
}


def _format_bytes(b):
    if b >= 1073741824:
        return f"{b / 1073741824:.2f} GB"
    elif b >= 1048576:
        return f"{b / 1048576:.2f} MB"
    elif b >= 1024:
        return f"{b / 1024:.2f} KB"
    return f"{b:.0f} B"


def _build_topic_section():
    state = get_state()
    topic_metrics = state.get("topic_metrics", {})
    if not topic_metrics or isinstance(topic_metrics, dict) and "error" in topic_metrics:
        return "No per-topic data available (requires PER_TOPIC_PER_BROKER monitoring level)"

    total_bytes = sum(t.get("bytes_in_per_sec", 0) for t in topic_metrics.values())
    lines = [f"Total ingress across all topics: {_format_bytes(total_bytes)}/s"]
    lines.append(f"Active topics: {len(topic_metrics)}")
    lines.append("")

    for i, (topic_name, tdata) in enumerate(topic_metrics.items()):
        if i >= 10:
            lines.append(f"... and {len(topic_metrics) - 10} more topics")
            break
        bytes_in = tdata.get("bytes_in_per_sec", 0)
        msgs_in = tdata.get("messages_in_per_sec", 0)
        avg = tdata.get("bytes_in_avg", 0)
        pct = (bytes_in / total_bytes * 100) if total_bytes > 0 else 0
        partitions = tdata.get("partition_count", 0)
        spike = ""
        if avg > 0 and bytes_in > avg * 1.5:
            spike = f" [SPIKE: {bytes_in/avg:.1f}x above average]"
        partition_info = f", {partitions} partitions" if partitions > 0 else ""
        lines.append(f"- Topic '{topic_name}': {_format_bytes(bytes_in)}/s ({msgs_in:.0f} msg/s{partition_info}) = {pct:.1f}% of total ingress (avg: {_format_bytes(avg)}/s){spike}")

    return chr(10).join(lines)


def _build_metrics_prompt():
    state = get_state()
    metrics = state.get("broker_metrics", {})
    cluster = state.get("cluster_info", {})
    health_score = state.get("health_score", "N/A")

    if not metrics or not cluster:
        return None

    metric_summaries = []

    tracked_metrics = [
        ("cpu", "CPU Usage", "%", False),
        ("cpu_system", "CPU System", "%", False),
        ("disk_used", "Storage Used", "auto", True),
        ("memory_used", "Memory Used", "bytes", True),
        ("memory_free", "Memory Free", "bytes", True),
        ("throughput_in", "Bytes In/sec", "bytes/s", True),
        ("throughput_out", "Bytes Out/sec", "bytes/s", True),
        ("messages_in", "Messages In/sec", "msg/s", False),
        ("produce_latency", "Produce Latency", "ms", False),
        ("fetch_latency", "Fetch Latency", "ms", False),
        ("partition_count", "Partition Count", "", False),
        ("topic_count", "Topic Count", "", False),
        ("under_replicated", "Under-Replicated Partitions", "", False),
        ("active_controller", "Active Controllers", "", False),
    ]

    for metric_key, display_name, unit, is_bytes in tracked_metrics:
        current = metrics.get(metric_key, {})
        if not current:
            continue

        value = current.get("value", 0)
        history = get_metric_history(metric_key)

        effective_unit = unit
        if unit == "auto" and metric_key == "disk_used":
            metric_unit_str = (current.get("unit", "") or "").lower()
            if "percent" in metric_unit_str or (0 <= value <= 100 and metric_unit_str != "bytes"):
                effective_unit = "%"
                is_bytes = False
            else:
                effective_unit = "bytes"

        def _fmt_metric(v, eu=effective_unit, ib=is_bytes):
            if ib and eu == "bytes":
                return _format_bytes(v)
            elif ib and "bytes/s" in eu:
                return _format_bytes(v) + "/s"
            elif eu == "%":
                return f"{v:.2f}%"
            elif eu == "ms":
                return f"{v:.2f} ms"
            elif eu == "msg/s":
                return f"{v:.1f} msg/s" if v < 10 else f"{v:.0f} msg/s"
            return f"{v:.2f} {eu}".strip()

        formatted_val = _fmt_metric(value)

        history_vals = []
        if history and len(history) > 1:
            for h in history[-10:]:
                history_vals.append(_fmt_metric(h["value"]))

        summary = f"- {display_name}: Current={formatted_val}"
        if history_vals:
            summary += f" | Recent history (oldest→newest): [{', '.join(history_vals)}]"
        metric_summaries.append(summary)

    broker_count = cluster.get("broker_count", "unknown")
    instance_type = cluster.get("instance_type", "unknown")
    kafka_version = cluster.get("kafka_version", "unknown")
    storage_gb = cluster.get("storage_per_broker_gb", "unknown")
    cluster_name = cluster.get("name", "unknown")

    prompt = f"""You are an expert Apache Kafka and AWS MSK operations engineer. Analyze the following real-time metrics from an MSK cluster and provide predictive analysis.

## Cluster Configuration
- Cluster: {cluster_name}
- Kafka Version: {kafka_version}
- Instance Type: {instance_type}
- Broker Count: {broker_count}
- Storage Per Broker: {storage_gb} GB
- Current Health Score: {health_score}/100

## Current Metrics (collected every 60 seconds)
{chr(10).join(metric_summaries)}

## Per-Topic Producer Activity
{_build_topic_section()}

## Instructions
Analyze these metrics and provide:

1. **Overall Assessment**: A brief 1-2 sentence health summary.
2. **Predictions**: For each key metric (CPU, Storage, Throughput, Latency), predict the trend over the next 1 hour, 6 hours, and 24 hours. Rate the risk level (low/medium/high/critical).
3. **Anomalies**: Flag any unusual patterns, spikes, or concerning trends. IMPORTANT: Identify which specific producer/topic is sending the most data and whether it is affecting cluster storage. If a producer is dominating ingress (>50% of total) or has spiked significantly, flag it as an anomaly with recommended throttling actions.
4. **Fetch Latency Root Cause Analysis**: If Fetch Latency is elevated (>100ms), diagnose the likely root causes. Consider: disk I/O contention, high partition count per broker, large consumer groups competing for resources, under-replicated partitions, broker CPU pressure, consumer fetch.max.bytes or max.poll.records config, page cache misses due to consumers reading old data, network saturation, or storage throughput limits. Identify the most likely cause given the other metrics.
5. **Recommendations**: Specific actionable recommendations based on the current state and predicted trends. If any producer is sending excessive data, recommend Kafka client quotas (producer_byte_rate) with specific throttle values. If fetch latency is elevated, provide specific tuning recommendations (consumer config, broker scaling, partition rebalancing). Prioritize by urgency.

Respond ONLY with valid JSON in this exact format:
{{
  "overall_assessment": "string",
  "predictions": [
    {{
      "metric": "string",
      "current_value": "string",
      "trend": "normal|increasing|decreasing|volatile",
      "risk_level": "low|medium|high|critical",
      "prediction_1h": "string",
      "prediction_6h": "string",
      "prediction_24h": "string",
      "explanation": "string"
    }}
  ],
  "anomalies": [
    {{
      "metric": "string",
      "description": "string",
      "severity": "info|warning|critical",
      "topic": "string or null - the topic name if this anomaly is about a specific producer/topic",
      "throttle_action": "string or null - specific throttle recommendation if producer needs limiting"
    }}
  ],
  "fetch_latency_analysis": {{
    "current_ms": "number - current fetch latency in ms",
    "is_elevated": "boolean - true if >100ms",
    "root_causes": ["string - list of likely root causes ranked by probability"],
    "primary_cause": "string - the single most likely cause given the metrics",
    "evidence": "string - which metrics or patterns support this diagnosis",
    "recommended_fix": "string - specific actionable fix for the primary cause"
  }},
  "recommendations": [
    {{
      "priority": "low|medium|high|critical",
      "title": "string",
      "description": "string",
      "action": "string"
    }}
  ]
}}"""

    return prompt


def analyze_with_bedrock():
    with _cache["lock"]:
        if _cache["last_analysis"] and _cache["last_updated"]:
            age = (datetime.utcnow() - _cache["last_updated"]).total_seconds()
            if age < 120:
                return _cache["last_analysis"]

    prompt = _build_metrics_prompt()
    if not prompt:
        return {"error": "No metrics data available yet. Wait for metrics collection."}

    try:
        bedrock = get_client("bedrock-runtime")

        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 2048, "temperature": 0.2},
        )

        ai_text = response["output"]["message"]["content"][0]["text"]

        ai_json = _parse_ai_json(ai_text)
        if ai_json is None:
            ai_json = {"overall_assessment": ai_text, "predictions": [], "anomalies": [], "recommendations": []}

        ai_json["analyzed_at"] = datetime.utcnow().isoformat() + "Z"
        ai_json["model"] = BEDROCK_MODEL_ID

        with _cache["lock"]:
            _cache["last_analysis"] = ai_json
            _cache["last_updated"] = datetime.utcnow()

        update_state("ai_predictions", ai_json)
        return ai_json

    except Exception as e:
        error_msg = str(e)
        return {
            "error": f"Bedrock analysis failed: {error_msg}",
            "analyzed_at": datetime.utcnow().isoformat() + "Z",
        }


def get_cached_analysis():
    with _cache["lock"]:
        if _cache["last_analysis"]:
            cached = _cache["last_analysis"]
            state = get_state()
            active_topics = set(state.get("topic_metrics", {}).keys())
            cached_topics = set()
            for rec in cached.get("recommendations", []):
                for t in rec.get("affected_topics", []):
                    cached_topics.add(t)
            for a in cached.get("anomalies", []):
                topic = a.get("topic", "")
                if topic:
                    cached_topics.add(topic)
            stale_topics = cached_topics - active_topics
            if stale_topics and not active_topics:
                _cache["last_analysis"] = None
                return {"error": "No AI analysis available yet. Click Refresh to run analysis."}
            return cached
    state = get_state()
    return state.get("ai_predictions", {"error": "No AI analysis available yet. Click Refresh to run analysis."})


_cost_cache = {
    "last_analysis": None,
    "last_updated": None,
    "lock": threading.Lock(),
}


def _build_cost_capacity_prompt():
    state = get_state()
    metrics = state.get("broker_metrics", {})
    cluster = state.get("cluster_info", {})

    if not metrics or not cluster:
        return None

    from services.cost_service import estimate_monthly_cost, INSTANCE_COSTS, STORAGE_COST_PER_GB_MONTH, EXPRESS_DATA_INGESTION_PER_GB
    cost_data = estimate_monthly_cost()

    broker_count = cluster.get("broker_count", "unknown")
    instance_type = cluster.get("instance_type", "unknown")
    kafka_version = cluster.get("kafka_version", "unknown")
    cluster_name = cluster.get("cluster_name", cluster.get("name", "unknown"))
    is_express = str(instance_type).startswith("express.")

    disk_metric = metrics.get("disk_used", {})
    disk_val = disk_metric.get("value", 0)
    disk_unit_str = (disk_metric.get("unit", "") or "").lower()
    disk_is_pct = "percent" in disk_unit_str or (0 <= disk_val <= 100 and disk_unit_str != "bytes")
    if disk_is_pct:
        storage_per_broker = cluster.get("storage_per_broker_gb", 5)
        disk_display = f"{disk_val:.2f}% ({(disk_val / 100.0) * storage_per_broker:.2f} GB of {storage_per_broker} GB)"
    else:
        disk_display = _format_bytes(disk_val)
    cpu_val = metrics.get("cpu", {}).get("value", 0)
    throughput_in = metrics.get("throughput_in", {}).get("value", 0)
    throughput_out = metrics.get("throughput_out", {}).get("value", 0)

    disk_history = []
    history = get_metric_history("disk_used")
    if history:
        for h in history[-20:]:
            if disk_is_pct:
                disk_history.append(f"{h['value']:.2f}%")
            else:
                disk_history.append(_format_bytes(h["value"]))

    throughput_history = []
    th_history = get_metric_history("throughput_in")
    if th_history:
        for h in th_history[-20:]:
            throughput_history.append(_format_bytes(h["value"]) + "/s")

    prompt = f"""You are an expert AWS MSK cost optimization engineer. Analyze the following cluster data and provide detailed cost optimization recommendations.

## Cluster Configuration
- Cluster: {cluster_name}
- Kafka Version: {kafka_version}
- Instance Type: {instance_type}
- Broker Type: {"Express" if is_express else "Standard"}
- Broker Count: {broker_count}
- Storage Type: {"Elastic (auto-scaling, pay-as-you-go)" if is_express else "Provisioned"}

## Current Cost Breakdown
- Compute Cost: ${cost_data.get('compute_cost_monthly', 0)}/month (${cost_data.get('hourly_rate_per_broker', 0)}/hr per broker x {broker_count} brokers x 730 hrs)
- Storage Cost: ${cost_data.get('storage_cost_monthly', 0)}/month
{"- Data Ingestion Cost: $" + str(cost_data.get('data_ingestion_cost_monthly', 0)) + "/month (" + str(cost_data.get('estimated_monthly_ingestion_gb', 0)) + " GB/month at $0.01/GB)" if is_express else ""}
- Total Estimated: ${cost_data.get('total_estimated_monthly', 0)}/month

## Current Metrics
- CPU Usage: {cpu_val:.1f}%
- Storage Used: {disk_display}
- Throughput In (avg per broker): {_format_bytes(throughput_in)}/s
- Throughput Out (avg per broker): {_format_bytes(throughput_out)}/s

## Storage History (oldest to newest, collected every ~60s)
{', '.join(disk_history) if disk_history else 'No history available'}

## Throughput History (oldest to newest)
{', '.join(throughput_history) if throughput_history else 'No history available'}

## Available Instance Types and Pricing (per hour)
{"Express: express.m7g.large=$0.408, express.m7g.4xlarge=$1.632, express.m7g.16xlarge=$6.528" if is_express else "Standard: kafka.t3.small=$0.067, kafka.m5.large=$0.21, kafka.m5.xlarge=$0.42, kafka.m5.2xlarge=$0.84, kafka.m5.4xlarge=$1.68, kafka.m7g.large=$0.204, kafka.m7g.xlarge=$0.408"}

## Instructions
Analyze the cost data and provide:

1. **Cost Assessment**: Overall cost efficiency rating and summary. Is this cluster cost-effective for its workload?
2. **Cost Optimizations**: Specific actionable ways to reduce costs. Consider instance rightsizing, storage optimization, retention policies, compression, and broker count.
3. **Scaling Recommendations**: When and how to scale - add/remove brokers, change instance type, adjust storage.
4. **Cost Projection**: Project costs for the next 3 months based on growth trends.

Respond ONLY with valid JSON in this exact format:
{{
  "cost_assessment": {{
    "rating": "excellent|good|fair|poor",
    "summary": "string - overall cost efficiency assessment",
    "monthly_cost": "string - current monthly cost summary",
    "cost_per_gb_ingested": "string - cost efficiency metric"
  }},
  "cost_optimizations": [
    {{
      "priority": "high|medium|low",
      "title": "string",
      "description": "string - detailed explanation",
      "estimated_savings": "string - estimated monthly savings",
      "implementation": "string - how to implement this optimization",
      "risk": "low|medium|high"
    }}
  ],
  "scaling_recommendations": [
    {{
      "timeframe": "string - when to act",
      "action": "string - what to do",
      "reason": "string - why",
      "impact": "string - expected cost/performance impact"
    }}
  ],
  "cost_projection": {{
    "current_monthly": "string",
    "month_1": "string - projected cost next month",
    "month_2": "string - projected cost in 2 months",
    "month_3": "string - projected cost in 3 months",
    "trend": "string - cost trend explanation"
  }}
}}"""

    return prompt


def analyze_cost_with_bedrock():
    with _cost_cache["lock"]:
        if _cost_cache["last_analysis"] and _cost_cache["last_updated"]:
            age = (datetime.utcnow() - _cost_cache["last_updated"]).total_seconds()
            if age < 120:
                return _cost_cache["last_analysis"]

    prompt = _build_cost_capacity_prompt()
    if not prompt:
        return {"error": "No metrics data available yet. Wait for metrics collection."}

    try:
        bedrock = get_client("bedrock-runtime")

        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 2048, "temperature": 0.2},
        )

        ai_text = response["output"]["message"]["content"][0]["text"]

        ai_json = _parse_ai_json(ai_text)
        if ai_json is None:
            ai_json = {"cost_assessment": {"rating": "unknown", "summary": ai_text}}

        ai_json["analyzed_at"] = datetime.utcnow().isoformat() + "Z"
        ai_json["model"] = BEDROCK_MODEL_ID

        with _cost_cache["lock"]:
            _cost_cache["last_analysis"] = ai_json
            _cost_cache["last_updated"] = datetime.utcnow()

        update_state("ai_cost_analysis", ai_json)
        return ai_json

    except Exception as e:
        error_msg = str(e)
        return {
            "error": f"Bedrock cost analysis failed: {error_msg}",
            "analyzed_at": datetime.utcnow().isoformat() + "Z",
        }


def get_cached_cost_analysis():
    with _cost_cache["lock"]:
        if _cost_cache["last_analysis"]:
            return _cost_cache["last_analysis"]
    state = get_state()
    return state.get("ai_cost_analysis", {"error": "No AI cost analysis available yet. Click 'Run AI Cost Analysis' to generate."})
