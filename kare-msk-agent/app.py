import os
from flask import Flask, render_template, jsonify, request
from services.aws_client import credentials_configured, refresh_clients, get_cluster_arn
from services import msk_service, cloudwatch_service, insights_service, remediation_service, cost_service, capacity_service, bedrock_service
from storage.state import get_state, add_alert, get_metric_history
from scheduler.jobs import scheduler

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "msk-agent-dev-key")


@app.after_request
def add_cache_headers(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/status")
def api_status():
    configured = credentials_configured()
    state = get_state()
    return jsonify({
        "configured": configured,
        "cluster_arn": get_cluster_arn() if configured else None,
        "last_error": state.get("last_error"),
    })


@app.route("/api/cluster")
def api_cluster():
    data = msk_service.get_cluster_info()
    return jsonify(data)


@app.route("/api/cluster/nodes")
def api_cluster_nodes():
    data = msk_service.list_nodes()
    return jsonify(data)


@app.route("/api/cluster/brokers")
def api_brokers():
    data = msk_service.get_bootstrap_brokers()
    return jsonify(data)


@app.route("/api/cluster/operations")
def api_operations():
    data = msk_service.get_cluster_operations()
    return jsonify(data)


@app.route("/api/metrics")
def api_metrics():
    state = get_state()
    return jsonify({
        "metrics": state.get("broker_metrics", {}),
        "updated": state.get("broker_metrics_updated"),
    })


@app.route("/api/metrics/refresh", methods=["POST"])
def api_metrics_refresh():
    data = cloudwatch_service.get_broker_metrics()
    return jsonify(data)


@app.route("/api/metrics/history/<metric_name>")
def api_metric_history(metric_name):
    from storage.state import get_metric_history
    data = get_metric_history(metric_name)
    return jsonify(data)


@app.route("/api/health")
def api_health():
    data = insights_service.analyze_cluster_health()
    return jsonify(data)


@app.route("/api/anomalies")
def api_anomalies():
    data = insights_service.detect_anomalies()
    return jsonify(data)


@app.route("/api/topic-metrics")
def api_topic_metrics():
    state = get_state()
    topic_metrics = state.get("topic_metrics", {})
    status = state.get("topic_metrics_status", "pending")
    updated = state.get("topic_metrics_updated")
    return jsonify({
        "topics": topic_metrics,
        "status": status,
        "updated_at": updated,
        "topic_count": len(topic_metrics) if isinstance(topic_metrics, dict) else 0,
    })


@app.route("/api/rca")
def api_rca():
    data = insights_service.generate_root_cause_analysis()
    return jsonify(data)


@app.route("/api/recommendations")
def api_recommendations():
    data = insights_service.get_optimization_recommendations()
    return jsonify(data)


@app.route("/api/ai-analysis")
def api_ai_analysis():
    data = bedrock_service.get_cached_analysis()
    return jsonify(data)


@app.route("/api/ai-analysis/refresh", methods=["POST"])
def api_ai_analysis_refresh():
    data = bedrock_service.analyze_with_bedrock()
    return jsonify(data)


@app.route("/api/ai-analysis/clear", methods=["POST"])
def api_ai_analysis_clear():
    from services.bedrock_service import _cache
    from storage.state import update_state as _update_state
    with _cache["lock"]:
        _cache["last_analysis"] = None
    _update_state("ai_predictions", None)
    return jsonify({"status": "cleared", "message": "AI analysis cache cleared."})


@app.route("/api/alerts")
def api_alerts():
    state = get_state()
    return jsonify(list(state.get("alerts", [])))


@app.route("/api/insights")
def api_insights():
    state = get_state()
    return jsonify(list(state.get("ai_insights", [])))


@app.route("/api/remediation/actions")
def api_remediation_actions():
    data = remediation_service.get_available_actions()
    return jsonify(data)


@app.route("/api/remediation/execute", methods=["POST"])
def api_remediation_execute():
    body = request.json or {}
    action = body.get("action")
    params = body.get("params", {})
    if not action:
        return jsonify({"error": "Action is required"}), 400
    result = remediation_service.execute_remediation(action, params)
    return jsonify(result)


@app.route("/api/remediation/log")
def api_remediation_log():
    state = get_state()
    return jsonify(list(state.get("remediation_log", [])))


@app.route("/api/remediation/ai-recommendations")
def api_ai_recommendations():
    data = remediation_service.get_ai_recommended_actions()
    return jsonify(data)


@app.route("/api/auto-agent/status")
def api_auto_agent_status():
    from services.auto_remediation_agent import get_agent_status
    return jsonify(get_agent_status())


@app.route("/api/auto-agent/toggle", methods=["POST"])
def api_auto_agent_toggle():
    from services.auto_remediation_agent import set_agent_enabled
    data = request.get_json() or {}
    enabled = data.get("enabled", True)
    return jsonify(set_agent_enabled(enabled))


@app.route("/api/auto-agent/evaluate", methods=["POST"])
def api_auto_agent_evaluate():
    from services.auto_remediation_agent import evaluate_and_remediate
    result = evaluate_and_remediate()
    return jsonify(result)


@app.route("/api/auto-agent/history")
def api_auto_agent_history():
    from services.auto_remediation_agent import get_agent_history
    return jsonify(get_agent_history())


@app.route("/api/cost")
def api_cost():
    data = cost_service.get_cost_recommendations()
    return jsonify(data)


@app.route("/api/capacity")
def api_capacity():
    data = capacity_service.get_capacity_summary()
    return jsonify(data)


@app.route("/api/ai-cost-analysis")
def api_ai_cost_analysis():
    data = bedrock_service.get_cached_cost_analysis()
    return jsonify(data)


@app.route("/api/ai-cost-analysis/refresh", methods=["POST"])
def api_ai_cost_analysis_refresh():
    data = bedrock_service.analyze_cost_with_bedrock()
    return jsonify(data)


@app.route("/api/logs")
def api_logs():
    log_group = request.args.get("log_group", "/aws/msk/demo-cluster-1")
    limit = int(request.args.get("limit", 50))
    data = cloudwatch_service.get_log_events(log_group, limit)
    return jsonify(data)


@app.route("/api/alarms")
def api_alarms():
    data = cloudwatch_service.get_cloudwatch_alarms()
    return jsonify(data)


@app.route("/api/test/invoke", methods=["POST"])
def api_test_invoke():
    from services.aws_client import get_client
    body = request.json or {}
    test_type = body.get("test_type")
    params = body.get("params", {})

    function_map = {
        "storage_growth": "msk-test-storage-growth",
        "dominant_producer": "msk-test-dominant-producer",
        "consume_lag": "msk-test-consume-lag",
        "consume_all": "msk-test-consume-all",
    }

    func_name = function_map.get(test_type)
    if not func_name:
        return jsonify({"error": f"Unknown test type: {test_type}"}), 400

    try:
        import json as json_mod
        lam = get_client("lambda")
        response = lam.invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=json_mod.dumps(params).encode(),
        )
        status_code = response.get("StatusCode", 0)
        if status_code == 202:
            return jsonify({
                "status": "invoked",
                "function": func_name,
                "message": f"Test '{test_type}' started. It will run in the background. Monitor the dashboard for anomalies.",
            })
        else:
            return jsonify({"error": f"Unexpected status: {status_code}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/clear-lag-data", methods=["POST"])
def api_clear_lag_data():
    from storage.state import clear_consumer_lag_data
    clear_consumer_lag_data()
    return jsonify({"status": "cleared", "message": "Consumer lag data, alerts, and remediation log entries cleared from dashboard."})


@app.route("/api/clear-anomalies", methods=["POST"])
def api_clear_anomalies():
    from storage.state import update_state, get_state
    from services.auto_remediation_agent import clear_agent_history
    state = get_state()
    current_topics = state.get("topic_metrics", {})
    suppressed = list(state.get("suppressed_topics", []))
    for topic in current_topics:
        if topic not in suppressed:
            suppressed.append(topic)
    update_state("suppressed_topics", suppressed)
    update_state("topic_metrics", {})
    update_state("topic_metrics_updated", None)
    update_state("anomalies", [])
    update_state("remediation_log", [])
    clear_agent_history()
    return jsonify({"status": "cleared", "message": "Anomaly detection, agent history, and remediation log cleared."})


@app.route("/api/test/status")
def api_test_status():
    from services.aws_client import get_client
    function_names = [
        "msk-test-storage-growth",
        "msk-test-dominant-producer",
        "msk-test-consume-lag",
        "msk-test-consume-all",
    ]
    results = {}
    try:
        lam = get_client("lambda")
        for fname in function_names:
            try:
                resp = lam.get_function(FunctionName=fname)
                config = resp.get("Configuration", {})
                results[fname] = {
                    "deployed": True,
                    "state": config.get("State", "Unknown"),
                    "last_modified": config.get("LastModified", ""),
                }
            except Exception:
                results[fname] = {"deployed": False}
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(results)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    refresh_clients()
    scheduler.run_once()
    return jsonify({"status": "refreshed"})


@app.route("/api/dashboard")
def api_dashboard():
    state = get_state()
    disk_history = get_metric_history("disk_used")
    disk_trend = None
    if len(disk_history) >= 2:
        vals = [h["value"] for h in disk_history]
        latest = vals[-1]
        oldest = vals[0]
        diff = latest - oldest
        if diff > 0:
            disk_trend = {"direction": "increasing", "change_value": diff, "change_bytes": diff, "points": len(vals)}
        elif diff < 0:
            disk_trend = {"direction": "decreasing", "change_value": abs(diff), "change_bytes": abs(diff), "points": len(vals)}
        else:
            disk_trend = {"direction": "stable", "change_value": 0, "change_bytes": 0, "points": len(vals)}
    return jsonify({
        "cluster": state.get("cluster_info"),
        "cluster_updated": state.get("cluster_info_updated"),
        "metrics": state.get("broker_metrics", {}),
        "metrics_updated": state.get("broker_metrics_updated"),
        "health_score": state.get("health_score", 100),
        "alerts": list(state.get("alerts", []))[:10],
        "insights": list(state.get("ai_insights", []))[:5],
        "remediation_log": list(state.get("remediation_log", []))[:5],
        "cost_insights": state.get("cost_insights"),
        "capacity_forecast": state.get("capacity_forecast"),
        "disk_trend": disk_trend,
    })


if __name__ == "__main__":
    print("Starting KARE Dashboard...")
    if credentials_configured():
        print("AWS credentials configured. Starting background monitoring...")
        scheduler.start()
    else:
        print("WARNING: AWS credentials not fully configured.")
        print("Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and MSK_CLUSTER_ARN in Secrets.")

    app.run(host="0.0.0.0", port=5000, debug=False)
