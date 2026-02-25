import threading
import time
import json
import os
from collections import deque
from datetime import datetime

SUPPRESSED_FILE = os.path.join(os.path.dirname(__file__), ".suppressed_topics.json")


def _load_suppressed_topics():
    try:
        if os.path.exists(SUPPRESSED_FILE):
            with open(SUPPRESSED_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return []


def _save_suppressed_topics(topics):
    try:
        with open(SUPPRESSED_FILE, "w") as f:
            json.dump(topics, f)
    except Exception:
        pass

_lock = threading.Lock()

_state = {
    "cluster_info": None,
    "cluster_info_updated": None,
    "broker_metrics": {},
    "broker_metrics_updated": None,
    "consumer_groups": [],
    "consumer_groups_updated": None,
    "topics": [],
    "topics_updated": None,
    "alerts": deque(maxlen=100),
    "ai_insights": deque(maxlen=50),
    "remediation_log": deque(maxlen=50),
    "metric_history": {
        "cpu": deque(maxlen=60),
        "cpu_system": deque(maxlen=60),
        "memory_used": deque(maxlen=60),
        "memory_free": deque(maxlen=60),
        "disk_used": deque(maxlen=60),
        "root_disk_used": deque(maxlen=60),
        "throughput_in": deque(maxlen=60),
        "throughput_out": deque(maxlen=60),
        "messages_in": deque(maxlen=60),
        "consumer_lag": deque(maxlen=60),
        "offset_lag": deque(maxlen=60),
        "sum_offset_lag": deque(maxlen=60),
        "under_replicated": deque(maxlen=60),
        "offline_partitions": deque(maxlen=60),
        "partition_count": deque(maxlen=60),
        "topic_count": deque(maxlen=60),
        "active_controller": deque(maxlen=60),
        "produce_latency": deque(maxlen=60),
        "fetch_latency": deque(maxlen=60),
    },
    "health_score": 100,
    "cost_insights": None,
    "capacity_forecast": None,
    "last_error": None,
    "suppressed_topics": _load_suppressed_topics(),
}


def get_state():
    with _lock:
        return _state.copy()


def update_state(key, value):
    with _lock:
        _state[key] = value
        if key == "suppressed_topics":
            _save_suppressed_topics(value)


def add_alert(severity, title, message, source="system"):
    with _lock:
        _state["alerts"].appendleft({
            "id": f"alert-{int(time.time() * 1000)}",
            "severity": severity,
            "title": title,
            "message": message,
            "source": source,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "acknowledged": False,
        })


def add_insight(category, title, description, recommendations=None):
    with _lock:
        _state["ai_insights"].appendleft({
            "id": f"insight-{int(time.time() * 1000)}",
            "category": category,
            "title": title,
            "description": description,
            "recommendations": recommendations or [],
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })


def add_remediation_log(action, status, details=""):
    with _lock:
        _state["remediation_log"].appendleft({
            "action": action,
            "status": status,
            "details": details,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })


def append_metric(metric_name, value):
    with _lock:
        if metric_name in _state["metric_history"]:
            _state["metric_history"][metric_name].append({
                "value": value,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            })


def get_metric_history(metric_name):
    with _lock:
        if metric_name in _state["metric_history"]:
            return list(_state["metric_history"][metric_name])
        return []


def clear_consumer_lag_data():
    with _lock:
        if "consumer_lag" in _state["metric_history"]:
            _state["metric_history"]["consumer_lag"].clear()
        if "offset_lag" in _state["metric_history"]:
            _state["metric_history"]["offset_lag"].clear()
        if "sum_offset_lag" in _state["metric_history"]:
            _state["metric_history"]["sum_offset_lag"].clear()

        broker_metrics = _state.get("broker_metrics", {})
        if "consumer_lag" in broker_metrics:
            broker_metrics["consumer_lag"] = {"value": 0, "unit": "ms"}
        if "offset_lag" in broker_metrics:
            broker_metrics["offset_lag"] = {"value": 0, "unit": "offsets"}

        new_alerts = deque(maxlen=100)
        for alert in _state["alerts"]:
            title = str(alert.get("title", "")).lower()
            msg = str(alert.get("message", "")).lower()
            if "lag" not in title and "lag" not in msg:
                new_alerts.append(alert)
        _state["alerts"] = new_alerts

        new_log = deque(maxlen=50)
        for entry in _state["remediation_log"]:
            action = str(entry.get("action", "")).lower()
            details = str(entry.get("details", "")).lower()
            if "lag" not in action and "lag-test" not in details:
                new_log.append(entry)
        _state["remediation_log"] = new_log

        new_insights = deque(maxlen=50)
        for insight in _state["ai_insights"]:
            title = str(insight.get("title", "")).lower()
            desc = str(insight.get("description", "")).lower()
            if "lag" not in title and "lag" not in desc:
                new_insights.append(insight)
        _state["ai_insights"] = new_insights
