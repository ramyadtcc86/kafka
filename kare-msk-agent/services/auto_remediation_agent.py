import json
import re
import threading
from datetime import datetime
from services.aws_client import get_client
from services.msk_service import get_bootstrap_brokers, update_broker_storage
from storage.state import get_state, update_state, add_remediation_log, add_alert, get_metric_history


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
    cleaned = re.sub(r':\s*true\b', ': true', cleaned)
    cleaned = re.sub(r':\s*false\b', ': false', cleaned)
    cleaned = re.sub(r':\s*null\b', ': null', cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    cleaned2 = re.sub(r'(?<=\d)\s*//.*?$', '', cleaned, flags=re.MULTILINE)
    cleaned2 = re.sub(r'([{,])\s*(\w+)\s*:', r'\1 "\2":', cleaned2)
    try:
        return json.loads(cleaned2)
    except json.JSONDecodeError:
        return None

BEDROCK_MODEL_ID = "meta.llama3-70b-instruct-v1:0"
PRODUCER_IMPACT_THRESHOLD = 50
STORAGE_UTILIZATION_THRESHOLD = 50
AUTO_REMEDIATION_ENABLED = True

_agent_state = {
    "lock": threading.Lock(),
    "enabled": True,
    "last_evaluation": None,
    "evaluation_count": 0,
    "actions_taken": [],
    "actions_skipped": [],
    "cooldown_until": {},
}

COOLDOWN_SECONDS = 300
STORAGE_COOLDOWN_SECONDS = 600


def _format_bytes(b):
    if b >= 1073741824:
        return f"{b / 1073741824:.1f} GB"
    elif b >= 1048576:
        return f"{b / 1048576:.1f} MB"
    elif b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b:.0f} B"


def get_agent_status():
    with _agent_state["lock"]:
        return {
            "enabled": _agent_state["enabled"],
            "last_evaluation": _agent_state["last_evaluation"],
            "evaluation_count": _agent_state["evaluation_count"],
            "actions_taken": list(_agent_state["actions_taken"][-20:]),
            "actions_skipped": list(_agent_state["actions_skipped"][-20:]),
            "threshold": f"{PRODUCER_IMPACT_THRESHOLD}%",
        }


def clear_agent_history():
    with _agent_state["lock"]:
        _agent_state["actions_taken"] = []
        _agent_state["actions_skipped"] = []
        _agent_state["cooldown_until"] = {}


def set_agent_enabled(enabled):
    with _agent_state["lock"]:
        _agent_state["enabled"] = enabled
    return {"enabled": enabled}


def _is_on_cooldown(action_key):
    with _agent_state["lock"]:
        cooldown_time = _agent_state["cooldown_until"].get(action_key)
        if cooldown_time and datetime.utcnow() < cooldown_time:
            return True
        return False


def _set_cooldown(action_key, seconds=None):
    from datetime import timedelta
    cooldown = seconds or COOLDOWN_SECONDS
    with _agent_state["lock"]:
        _agent_state["cooldown_until"][action_key] = datetime.utcnow() + timedelta(seconds=cooldown)


def _log_action(action_type, details, executed=True):
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "action_type": action_type,
        "details": details,
        "executed": executed,
    }
    with _agent_state["lock"]:
        if executed:
            _agent_state["actions_taken"].append(entry)
        else:
            _agent_state["actions_skipped"].append(entry)

    status = "executed" if executed else "skipped (safety)"
    add_remediation_log(
        f"Auto-Agent: {action_type}",
        status,
        details
    )


def _ask_ai_safety_check(topic_name, pct_of_total, bytes_in_per_sec, cluster_info, broker_metrics, proposed_action):
    try:
        bedrock = get_client("bedrock-runtime")

        cpu_val = broker_metrics.get("cpu", {}).get("value", 0)
        fetch_latency = broker_metrics.get("fetch_latency", {}).get("value", 0)
        throughput_in = broker_metrics.get("throughput_in", {}).get("value", 0)
        broker_count = cluster_info.get("broker_count", 0)
        instance_type = cluster_info.get("instance_type", "")

        prompt = f"""You are an MSK cluster safety evaluator. A producer on topic '{topic_name}' is consuming {pct_of_total:.1f}% of total cluster ingress at {_format_bytes(bytes_in_per_sec)}/s.

Current cluster state:
- Instance type: {instance_type}
- Broker count: {broker_count}
- CPU usage: {cpu_val:.1f}%
- Fetch latency: {fetch_latency:.1f} ms
- Total throughput in: {_format_bytes(throughput_in)}/s

Proposed auto-remediation action:
{proposed_action}

IMPORTANT: Evaluate whether executing this throttle action is SAFE for the cluster. Consider:
1. Will throttling this producer cause message backlog and potential data loss?
2. Is the cluster under enough pressure that throttling is necessary?
3. Could throttling cause cascading failures (e.g., if this is a critical system topic)?
4. Is the proposed byte rate limit reasonable (not too aggressive)?

Respond in this exact JSON format:
{{
  "safe_to_execute": true or false,
  "confidence": 0.0 to 1.0,
  "reasoning": "brief explanation of why it is or is not safe",
  "risk_level": "low" or "medium" or "high",
  "suggested_byte_rate": integer bytes per second (your recommended limit),
  "warnings": ["list of any concerns"]
}}

Only set safe_to_execute to true if you are confident (>0.7) the action will not negatively impact the cluster."""

        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 1024, "temperature": 0.1},
        )

        ai_text = response["output"]["message"]["content"][0]["text"]

        result = _parse_ai_json(ai_text)
        if result is None:
            result = {
                "safe_to_execute": False,
                "confidence": 0.0,
                "reasoning": "Could not parse AI response; defaulting to safe mode (no action)",
                "risk_level": "high",
                "warnings": ["AI response parsing failed"],
            }

        return result

    except Exception as e:
        return {
            "safe_to_execute": False,
            "confidence": 0.0,
            "reasoning": f"AI safety check failed: {str(e)}. Defaulting to safe mode (no action).",
            "risk_level": "high",
            "warnings": [str(e)],
        }


def _ask_ai_storage_safety_check(current_gb, storage_limit_gb, utilization_pct, growth_rate_bytes, cluster_info, broker_metrics, proposed_target_gb):
    try:
        bedrock = get_client("bedrock-runtime")

        cpu_val = broker_metrics.get("cpu", {}).get("value", 0)
        throughput_in = broker_metrics.get("throughput_in", {}).get("value", 0)
        broker_count = cluster_info.get("broker_count", 0)
        instance_type = cluster_info.get("instance_type", "")
        is_express = "express" in instance_type.lower() if instance_type else False

        growth_per_hour = abs(growth_rate_bytes) * 60 if growth_rate_bytes > 0 else 0
        hours_to_full = ((storage_limit_gb * 1073741824 - current_gb * 1073741824) / growth_rate_bytes / 60) if growth_rate_bytes > 0 else float('inf')

        prompt = f"""You are an MSK cluster safety evaluator. Storage utilization is at {utilization_pct:.1f}% ({current_gb:.1f} GB used out of {storage_limit_gb} GB limit per broker).

Current cluster state:
- Instance type: {instance_type}
- Broker count: {broker_count}
- CPU usage: {cpu_val:.1f}%
- Total throughput in: {_format_bytes(throughput_in)}/s
- Storage growth rate: {_format_bytes(growth_per_hour)}/hour
- Estimated time to full: {hours_to_full:.0f} hours
- Broker type: {"Express (elastic storage)" if is_express else "Standard (provisioned EBS)"}

Proposed auto-remediation action:
Expand broker storage from {current_gb:.0f} GB to {proposed_target_gb} GB per broker.

IMPORTANT: Evaluate whether executing this storage expansion is SAFE for the cluster. Consider:
1. Is storage utilization high enough to warrant expansion?
2. Will expansion cause any downtime or performance impact?
3. Is the proposed target size reasonable (not too aggressive or too small)?
4. Is there an underlying issue (runaway producer) that should be fixed first instead of just expanding storage?
5. For Express brokers, storage auto-scales so manual expansion may not be needed.

Respond in this exact JSON format:
{{
  "safe_to_execute": true or false,
  "confidence": 0.0 to 1.0,
  "reasoning": "brief explanation of why expansion is or is not recommended",
  "risk_level": "low" or "medium" or "high",
  "suggested_target_gb": integer (your recommended target size in GB),
  "warnings": ["list of any concerns"]
}}

Only set safe_to_execute to true if you are confident (>0.7) the action will benefit the cluster without negative impact."""

        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 1024, "temperature": 0.1},
        )

        ai_text = response["output"]["message"]["content"][0]["text"]

        result = _parse_ai_json(ai_text)
        if result is None:
            result = {
                "safe_to_execute": False,
                "confidence": 0.0,
                "reasoning": "Could not parse AI response; defaulting to safe mode (no action)",
                "risk_level": "high",
                "warnings": ["AI response parsing failed"],
            }

        return result

    except Exception as e:
        return {
            "safe_to_execute": False,
            "confidence": 0.0,
            "reasoning": f"AI safety check failed: {str(e)}. Defaulting to safe mode (no action).",
            "risk_level": "high",
            "warnings": [str(e)],
        }


def _evaluate_storage(state, cluster_info, broker_metrics):
    disk_metric = broker_metrics.get("disk_used", {})
    disk_value = disk_metric.get("value", 0)
    disk_unit = disk_metric.get("unit", "").lower()

    instance_type = cluster_info.get("instance_type", "")
    is_express = "express" in instance_type.lower() if instance_type else False

    if is_express:
        return None

    storage_limit_gb = cluster_info.get("storage_per_broker_gb", 100)
    if storage_limit_gb <= 0:
        storage_limit_gb = 100

    if "percent" in disk_unit or (0 < disk_value <= 100 and disk_unit != "bytes"):
        utilization_pct = disk_value
        disk_gb = (utilization_pct / 100) * storage_limit_gb
    else:
        disk_gb = disk_value / (1024 ** 3)
        utilization_pct = (disk_gb / storage_limit_gb) * 100

    ai_predicted_storage_issue = False
    ai_analysis = state.get("ai_analysis", {})
    anomalies = state.get("anomalies", [])
    ai_recommendations = state.get("ai_recommendations", {})

    if isinstance(ai_analysis, dict) and ai_analysis.get("predictions"):
        for pred in ai_analysis.get("predictions", []):
            pred_text = str(pred).lower()
            if "storage" in pred_text and ("high" in pred_text or "above" in pred_text or "exceed" in pred_text or "grow" in pred_text or "50" in pred_text):
                ai_predicted_storage_issue = True
                break

    if isinstance(anomalies, list):
        for anomaly in anomalies:
            if isinstance(anomaly, dict):
                metric = str(anomaly.get("metric", "")).lower()
                severity = str(anomaly.get("severity", "")).lower()
                if ("storage" in metric or "disk" in metric) and severity in ("warning", "critical"):
                    ai_predicted_storage_issue = True
                    break

    if isinstance(ai_recommendations, dict):
        for rec in ai_recommendations.get("recommendations", []):
            if isinstance(rec, dict):
                rec_type = str(rec.get("type", "")).lower()
                rec_title = str(rec.get("title", "")).lower()
                if "storage" in rec_type or "storage" in rec_title or "expand" in rec_title:
                    ai_predicted_storage_issue = True
                    break

    health = state.get("health", {})
    if isinstance(health, dict):
        for issue in health.get("issues", []):
            if "storage" in str(issue).lower() or "disk" in str(issue).lower():
                ai_predicted_storage_issue = True
                break

    if utilization_pct < STORAGE_UTILIZATION_THRESHOLD and not ai_predicted_storage_issue:
        return None

    trigger_reason = f"Storage utilization at {utilization_pct:.1f}%"
    if ai_predicted_storage_issue and utilization_pct < STORAGE_UTILIZATION_THRESHOLD:
        trigger_reason = f"AI predicted storage issue (current: {utilization_pct:.1f}%, threshold: {STORAGE_UTILIZATION_THRESHOLD}%)"

    cooldown_key = "storage_expansion"
    if _is_on_cooldown(cooldown_key):
        return {
            "action_type": "storage_expansion",
            "status": "on_cooldown",
            "message": "Storage expansion is on cooldown. Skipping evaluation.",
            "utilization_pct": round(utilization_pct, 1),
            "current_gb": round(disk_gb, 1),
            "limit_gb": storage_limit_gb,
        }

    storage_history = get_metric_history("disk_used")
    growth_rate_pct_per_interval = 0
    growth_rate_bytes = 0
    if len(storage_history) >= 2:
        values = [h["value"] for h in storage_history]
        growth_rate_pct_per_interval = (values[-1] - values[0]) / max(len(values) - 1, 1)
        growth_rate_bytes = (growth_rate_pct_per_interval / 100) * storage_limit_gb * (1024 ** 3)

    proposed_target_gb = storage_limit_gb + 2

    print(f"[AutoAgent] {trigger_reason}. Storage: {disk_gb:.1f} GB / {storage_limit_gb} GB. Running AI safety check for expansion to {proposed_target_gb} GB...")

    safety_result = _ask_ai_storage_safety_check(
        disk_gb, storage_limit_gb, utilization_pct,
        growth_rate_bytes, cluster_info, broker_metrics, proposed_target_gb
    )

    proposed_target_gb = storage_limit_gb + 2

    is_safe = safety_result.get("safe_to_execute", False)
    confidence = safety_result.get("confidence", 0)
    reasoning = safety_result.get("reasoning", "No reasoning provided")
    risk_level = safety_result.get("risk_level", "unknown")
    warnings = safety_result.get("warnings", [])

    action_record = {
        "action_type": "storage_expansion",
        "trigger_reason": trigger_reason,
        "ai_predicted": ai_predicted_storage_issue,
        "utilization_pct": round(utilization_pct, 1),
        "current_gb": round(disk_gb, 1),
        "limit_gb": storage_limit_gb,
        "proposed_target_gb": proposed_target_gb,
        "growth_rate": _format_bytes(abs(growth_rate_bytes * 60)) + "/hour" if growth_rate_bytes > 0 else "stable",
        "ai_safety_check": {
            "safe_to_execute": is_safe,
            "confidence": confidence,
            "reasoning": reasoning,
            "risk_level": risk_level,
            "warnings": warnings,
        },
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    if is_safe and confidence >= 0.7:
        result = update_broker_storage(proposed_target_gb)

        if result.get("error"):
            action_record["status"] = "execution_failed"
            action_record["action_taken"] = True
            action_record["execution_error"] = result["error"]

            _log_action(
                "storage_expansion_failed",
                f"AI approved storage expansion to {proposed_target_gb} GB but execution failed: {result['error']}",
                executed=True
            )

            add_alert(
                "warning",
                "Auto-Remediation: Storage Expansion Failed",
                f"Storage at {utilization_pct:.1f}% ({disk_gb:.1f} GB / {storage_limit_gb} GB). "
                f"AI approved expansion to {proposed_target_gb} GB but execution failed: {result['error']}",
                source="Auto-Remediation Agent"
            )

            print(f"[AutoAgent] Storage expansion to {proposed_target_gb} GB FAILED: {result['error']}")
        else:
            action_record["status"] = "executed"
            action_record["action_taken"] = True
            action_record["execution_result"] = result

            _log_action(
                "storage_expansion",
                f"Storage expansion initiated: {storage_limit_gb} GB -> {proposed_target_gb} GB per broker. "
                f"Utilization was {utilization_pct:.1f}%. AI confidence: {confidence:.0%}. Risk: {risk_level}.",
                executed=True
            )

            add_alert(
                "info",
                "Auto-Remediation: Storage Expansion Initiated",
                f"Storage at {utilization_pct:.1f}% ({disk_gb:.1f} GB / {storage_limit_gb} GB). "
                f"Expanding to {proposed_target_gb} GB per broker. AI confidence: {confidence:.0%}.",
                source="Auto-Remediation Agent"
            )

            print(f"[AutoAgent] EXECUTED storage expansion: {storage_limit_gb} GB -> {proposed_target_gb} GB (AI confidence: {confidence:.0%})")

        _set_cooldown(cooldown_key, STORAGE_COOLDOWN_SECONDS)
    else:
        action_record["status"] = "skipped"
        action_record["action_taken"] = False
        skip_reason = f"AI safety check: safe={is_safe}, confidence={confidence:.0%}, risk={risk_level}. {reasoning}"
        action_record["skip_reason"] = skip_reason

        _log_action(
            "storage_expansion_skipped",
            f"Storage at {utilization_pct:.1f}% ({disk_gb:.1f} GB / {storage_limit_gb} GB). "
            f"Expansion skipped. {skip_reason}",
            executed=False
        )

        add_alert(
            "warning",
            "Auto-Remediation: Storage Expansion Skipped",
            f"Storage at {utilization_pct:.1f}% but AI determined expansion may not be needed. "
            f"Risk: {risk_level}, confidence: {confidence:.0%}. {reasoning}",
            source="Auto-Remediation Agent"
        )

        print(f"[AutoAgent] SKIPPED storage expansion: {skip_reason}")

    return action_record


def _generate_throttle_command(topic_name, byte_rate_limit, bootstrap_brokers):
    broker_string = bootstrap_brokers.get("tls") or bootstrap_brokers.get("plaintext") or bootstrap_brokers.get("sasl_iam") or "$BROKER_ENDPOINT"

    return {
        "command": f"kafka-configs.sh --bootstrap-server {broker_string} --alter --add-config 'producer_byte_rate={byte_rate_limit}' --entity-type clients --entity-name {topic_name}-producer",
        "byte_rate_limit": byte_rate_limit,
        "topic": topic_name,
        "broker_endpoint": broker_string,
    }


def evaluate_and_remediate():
    with _agent_state["lock"]:
        if not _agent_state["enabled"]:
            return {"status": "disabled", "message": "Auto-remediation agent is disabled"}

    state = get_state()
    topic_metrics = state.get("topic_metrics", {})
    suppressed_topics = state.get("suppressed_topics", [])
    topic_metrics = {k: v for k, v in topic_metrics.items() if k not in suppressed_topics}
    cluster_info = state.get("cluster_info", {})
    broker_metrics = state.get("broker_metrics", {})

    if not topic_metrics or not isinstance(topic_metrics, dict) or "error" in topic_metrics:
        return {"status": "no_data", "message": "No topic metrics available for evaluation"}

    if not cluster_info:
        return {"status": "no_cluster", "message": "No cluster info available"}

    total_bytes_in = sum(t.get("bytes_in_per_sec", 0) for t in topic_metrics.values())

    evaluation_results = []
    actions_proposed = []

    storage_action = _evaluate_storage(state, cluster_info, broker_metrics)
    if storage_action:
        evaluation_results.append(storage_action)
        if storage_action.get("status") != "on_cooldown":
            actions_proposed.append(storage_action)

    for topic_name, tdata in topic_metrics.items():
        bytes_in = tdata.get("bytes_in_per_sec", 0)
        if total_bytes_in < 100 or bytes_in < 100:
            continue

        pct_of_total = (bytes_in / total_bytes_in) * 100

        if pct_of_total < PRODUCER_IMPACT_THRESHOLD:
            continue

        cooldown_key = f"throttle_{topic_name}"
        if _is_on_cooldown(cooldown_key):
            evaluation_results.append({
                "topic": topic_name,
                "pct_of_total": pct_of_total,
                "status": "on_cooldown",
                "message": f"Topic '{topic_name}' is on cooldown. Skipping evaluation.",
            })
            continue

        suggested_byte_rate = max(1048576, int(bytes_in * 1.2))

        proposed_action = (
            f"Apply producer throttle on topic '{topic_name}' with byte rate limit of "
            f"{_format_bytes(suggested_byte_rate)}/s (current rate: {_format_bytes(bytes_in)}/s, "
            f"{pct_of_total:.1f}% of total ingress)"
        )

        print(f"[AutoAgent] Topic '{topic_name}' at {pct_of_total:.1f}% of ingress ({_format_bytes(bytes_in)}/s). Running AI safety check...")

        safety_result = _ask_ai_safety_check(
            topic_name, pct_of_total, bytes_in,
            cluster_info, broker_metrics, proposed_action
        )

        ai_suggested_rate = safety_result.get("suggested_byte_rate", suggested_byte_rate)
        if isinstance(ai_suggested_rate, (int, float)) and ai_suggested_rate > 0:
            suggested_byte_rate = int(ai_suggested_rate)

        is_safe = safety_result.get("safe_to_execute", False)
        confidence = safety_result.get("confidence", 0)
        reasoning = safety_result.get("reasoning", "No reasoning provided")
        risk_level = safety_result.get("risk_level", "unknown")
        warnings = safety_result.get("warnings", [])

        bootstrap = get_bootstrap_brokers()
        throttle_cmd = _generate_throttle_command(topic_name, suggested_byte_rate, bootstrap)

        action_record = {
            "topic": topic_name,
            "pct_of_total": round(pct_of_total, 1),
            "bytes_in_per_sec": bytes_in,
            "bytes_in_formatted": _format_bytes(bytes_in) + "/s",
            "proposed_byte_rate": suggested_byte_rate,
            "proposed_byte_rate_formatted": _format_bytes(suggested_byte_rate) + "/s",
            "throttle_command": throttle_cmd,
            "ai_safety_check": {
                "safe_to_execute": is_safe,
                "confidence": confidence,
                "reasoning": reasoning,
                "risk_level": risk_level,
                "warnings": warnings,
            },
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        if is_safe and confidence >= 0.7:
            bootstrap = get_bootstrap_brokers()
            throttle_cmd = _generate_throttle_command(topic_name, suggested_byte_rate, bootstrap)
            action_record["throttle_command"] = throttle_cmd
            action_record["status"] = "approved"
            action_record["action_taken"] = True

            _log_action(
                "producer_throttle_approved",
                f"AI approved throttle for topic '{topic_name}' ({pct_of_total:.1f}% of ingress) to {_format_bytes(suggested_byte_rate)}/s. "
                f"AI confidence: {confidence:.0%}. Risk: {risk_level}. Command ready for execution via Kafka CLI or Lambda.",
                executed=True
            )
            _set_cooldown(cooldown_key)

            add_alert(
                "info",
                f"Auto-Remediation: Throttle Approved for '{topic_name}'",
                f"Producer on topic '{topic_name}' consuming {pct_of_total:.1f}% of cluster ingress. "
                f"Throttle to {_format_bytes(suggested_byte_rate)}/s approved by AI (confidence: {confidence:.0%}). "
                f"Execute via Kafka CLI or VPC-connected Lambda.",
                source="Auto-Remediation Agent"
            )

            print(f"[AutoAgent] APPROVED throttle on '{topic_name}': {_format_bytes(suggested_byte_rate)}/s (AI confidence: {confidence:.0%})")
        else:
            action_record["status"] = "skipped"
            action_record["action_taken"] = False
            skip_reason = f"AI safety check: safe={is_safe}, confidence={confidence:.0%}, risk={risk_level}. {reasoning}"
            action_record["skip_reason"] = skip_reason

            _log_action(
                "producer_throttle_skipped",
                f"Skipped throttle on topic '{topic_name}' ({pct_of_total:.1f}% of ingress). {skip_reason}",
                executed=False
            )

            add_alert(
                "warning",
                f"Auto-Remediation: Action Skipped for '{topic_name}'",
                f"Producer on topic '{topic_name}' at {pct_of_total:.1f}% of ingress, but AI determined "
                f"throttling may impact cluster (risk: {risk_level}, confidence: {confidence:.0%}). "
                f"Recommendation: {reasoning}",
                source="Auto-Remediation Agent"
            )

            print(f"[AutoAgent] SKIPPED throttle on '{topic_name}': {skip_reason}")

        actions_proposed.append(action_record)
        evaluation_results.append(action_record)

    with _agent_state["lock"]:
        _agent_state["last_evaluation"] = datetime.utcnow().isoformat() + "Z"
        _agent_state["evaluation_count"] += 1

    update_state("auto_agent_last_evaluation", {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "results": evaluation_results,
        "total_topics_checked": len(topic_metrics),
        "topics_above_threshold": len(actions_proposed),
    })

    result = {
        "status": "evaluated",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "total_topics": len(topic_metrics),
        "topics_above_threshold": len(actions_proposed),
        "actions": actions_proposed,
    }

    if storage_action and storage_action.get("status") != "on_cooldown":
        result["storage_action"] = {
            "utilization_pct": storage_action.get("utilization_pct"),
            "current_gb": storage_action.get("current_gb"),
            "limit_gb": storage_action.get("limit_gb"),
            "proposed_target_gb": storage_action.get("proposed_target_gb"),
            "status": storage_action.get("status"),
        }

    return result


def get_agent_history():
    with _agent_state["lock"]:
        all_actions = list(_agent_state["actions_taken"][-20:]) + list(_agent_state["actions_skipped"][-20:])

    all_actions.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    state = get_state()
    last_eval = state.get("auto_agent_last_evaluation", {})

    return {
        "history": all_actions[:30],
        "last_evaluation": last_eval,
        "agent_enabled": _agent_state["enabled"],
        "threshold": f"{PRODUCER_IMPACT_THRESHOLD}%",
    }
