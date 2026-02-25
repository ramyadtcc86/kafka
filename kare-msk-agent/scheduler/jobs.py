import threading
import time
import traceback
from services.aws_client import credentials_configured


class MonitorScheduler:
    def __init__(self, interval=60):
        self.interval = interval
        self._running = False
        self._thread = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def _run_loop(self):
        while self._running:
            try:
                self._collect()
            except Exception as e:
                print(f"[Scheduler] Error in collection cycle: {e}")
                traceback.print_exc()
            time.sleep(self.interval)

    def _collect(self):
        if not credentials_configured():
            return

        from services.msk_service import get_cluster_info, list_nodes
        from services.cloudwatch_service import get_broker_metrics, get_per_topic_metrics
        from services.insights_service import analyze_cluster_health, detect_anomalies
        from services.capacity_service import forecast_storage

        print(f"[Scheduler] Running collection cycle...")

        cluster = get_cluster_info()
        if isinstance(cluster, dict) and "error" not in cluster:
            print(f"[Scheduler] Cluster: {cluster.get('cluster_name')} | State: {cluster.get('state')}")

        metrics = get_broker_metrics()
        if isinstance(metrics, dict) and metrics:
            cpu = metrics.get("cpu", {}).get("value", "N/A")
            print(f"[Scheduler] Metrics collected. CPU: {cpu}")

        from storage.state import get_state as get_current_state
        old_topics = set(get_current_state().get("topic_metrics", {}).keys())

        topic_data = get_per_topic_metrics()
        if isinstance(topic_data, dict) and "error" not in topic_data and topic_data:
            print(f"[Scheduler] Per-topic metrics collected for {len(topic_data)} topic(s)")

        new_topics = set(topic_data.keys()) if isinstance(topic_data, dict) and "error" not in topic_data else set()
        removed_topics = old_topics - new_topics
        if removed_topics:
            print(f"[Scheduler] Topics removed: {', '.join(removed_topics)} — clearing AI cache")
            from services.bedrock_service import _cache
            with _cache["lock"]:
                _cache["last_analysis"] = None

        health = analyze_cluster_health()
        print(f"[Scheduler] Health score: {health.get('health_score', 'N/A')}/100")

        anomalies = detect_anomalies()
        if anomalies:
            print(f"[Scheduler] Detected {len(anomalies)} anomaly(ies)")

        forecast_storage()

        from services.auto_remediation_agent import evaluate_and_remediate
        try:
            agent_result = evaluate_and_remediate()
            actions_count = len(agent_result.get("actions", []))
            executed = len([a for a in agent_result.get("actions", []) if a.get("action_taken")])
            storage_info = ""
            if agent_result.get("storage_action"):
                sa = agent_result["storage_action"]
                storage_info = f" | Storage: {sa.get('utilization_pct', 0)}% ({sa.get('status', 'n/a')})"
            if actions_count > 0:
                print(f"[Scheduler] Auto-Agent: {actions_count} issue(s) detected, {executed} action(s) taken{storage_info}")
            elif agent_result.get("status") == "evaluated":
                print(f"[Scheduler] Auto-Agent: All metrics within safe limits{storage_info}")
        except Exception as e:
            print(f"[Scheduler] Auto-Agent error: {e}")

    def run_once(self):
        try:
            self._collect()
        except Exception as e:
            print(f"[Scheduler] Error: {e}")


scheduler = MonitorScheduler(interval=60)
