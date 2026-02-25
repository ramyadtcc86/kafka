from services.aws_client import get_client, get_cluster_arn
from storage.state import update_state, get_state, append_metric, add_alert
from datetime import datetime, timedelta


METRIC_NAMESPACE = "AWS/Kafka"


def _get_metric_stats(metric_name, stat="Average", period=300, hours=1, dimensions=None):
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return None

    try:
        cw = get_client("cloudwatch")
        cluster_name = cluster_arn.split("/")[-2] if "/" in cluster_arn else "unknown"

        dims = dimensions or [
            {"Name": "Cluster Name", "Value": cluster_name},
        ]

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        response = cw.get_metric_statistics(
            Namespace=METRIC_NAMESPACE,
            MetricName=metric_name,
            Dimensions=dims,
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=[stat],
        )

        datapoints = response.get("Datapoints", [])
        datapoints.sort(key=lambda x: x["Timestamp"])
        return datapoints
    except Exception as e:
        return {"error": str(e)}


def get_broker_metrics():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {}

    cluster_name = cluster_arn.split("/")[-2] if "/" in cluster_arn else "unknown"

    broker_metrics = [
        ("CpuUser", "cpu", "Average"),
        ("CpuSystem", "cpu_system", "Average"),
        ("MemoryUsed", "memory_used", "Average"),
        ("MemoryFree", "memory_free", "Average"),
        ("BytesInPerSec", "throughput_in", "Sum"),
        ("BytesOutPerSec", "throughput_out", "Sum"),
        ("MessagesInPerSec", "messages_in", "Sum"),
        ("FetchConsumerTotalTimeMsMean", "fetch_latency", "Average"),
        ("ProduceTotalTimeMsMean", "produce_latency", "Average"),
        ("KafkaDataLogsDiskUsed", "disk_used", "Average"),
    ]

    cluster_metrics = [
        ("ActiveControllerCount", "active_controller", "Maximum"),
        ("GlobalPartitionCount", "partition_count", "Maximum"),
        ("GlobalTopicCount", "topic_count", "Maximum"),
    ]

    metrics = {}

    for broker_id in ["1", "2", "3"]:
        broker_dims = [
            {"Name": "Cluster Name", "Value": cluster_name},
            {"Name": "Broker ID", "Value": broker_id},
        ]
        for aws_name, local_name, stat in broker_metrics:
            data = _get_metric_stats(aws_name, stat=stat, dimensions=broker_dims)
            if data and not isinstance(data, dict) and data:
                latest = data[-1]
                value = latest.get(stat, 0)
                key = local_name
                if key in metrics:
                    brokers = metrics[key]["brokers"]
                    brokers[broker_id] = value
                    all_vals = list(brokers.values())
                    metrics[key]["value"] = sum(all_vals) / len(all_vals)
                else:
                    metrics[key] = {
                        "value": value,
                        "unit": latest.get("Unit", ""),
                        "timestamp": latest["Timestamp"].isoformat(),
                        "brokers": {broker_id: value},
                    }

    for aws_name, local_name, stat in cluster_metrics:
        data = _get_metric_stats(aws_name, stat=stat)
        if data and not isinstance(data, dict) and data:
            latest = data[-1]
            value = latest.get(stat, 0)
            metrics[local_name] = {
                "value": value,
                "unit": latest.get("Unit", ""),
                "timestamp": latest["Timestamp"].isoformat(),
            }

    for local_name in metrics:
        val = metrics[local_name]["value"]
        append_metric(local_name, val)

    _check_metric_alerts(metrics)

    update_state("broker_metrics", metrics)
    update_state("broker_metrics_updated", datetime.utcnow().isoformat() + "Z")

    return metrics


def _check_metric_alerts(metrics):
    if "cpu" in metrics and metrics["cpu"]["value"] > 80:
        add_alert("warning", "High CPU Usage",
                   f"CPU usage at {metrics['cpu']['value']:.1f}%", source="cloudwatch")

    if "disk_used" in metrics:
        disk_val = metrics["disk_used"]["value"]
        disk_unit = metrics["disk_used"].get("unit", "").lower()
        if "percent" in disk_unit or (0 < disk_val <= 100 and disk_unit != "bytes"):
            if disk_val > 80:
                add_alert("critical", "High Disk Usage",
                           f"Storage utilization at {disk_val:.1f}%", source="cloudwatch")
            elif disk_val > 50:
                add_alert("warning", "Elevated Disk Usage",
                           f"Storage utilization at {disk_val:.1f}%", source="cloudwatch")
        else:
            disk_gb = disk_val / (1024 ** 3)
            if disk_gb > 100:
                add_alert("critical", "High Disk Usage",
                           f"Storage used: {disk_gb:.1f} GB", source="cloudwatch")
            elif disk_gb > 50:
                add_alert("warning", "Elevated Disk Usage",
                           f"Storage used: {disk_gb:.1f} GB", source="cloudwatch")

    if "under_replicated" in metrics and metrics["under_replicated"]["value"] > 0:
        add_alert("critical", "Under-Replicated Partitions",
                   f"{int(metrics['under_replicated']['value'])} partitions are under-replicated",
                   source="cloudwatch")

    if "offline_partitions" in metrics and metrics["offline_partitions"]["value"] > 0:
        add_alert("critical", "Offline Partitions",
                   f"{int(metrics['offline_partitions']['value'])} partitions are offline",
                   source="cloudwatch")



def get_per_topic_metrics():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {}

    cluster_name = cluster_arn.split("/")[-2] if "/" in cluster_arn else "unknown"
    period = 300

    try:
        cw = get_client("cloudwatch")
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=1)

        topic_broker_map = {}
        broker_ids = set()

        paginator = cw.get_paginator("list_metrics")
        for page in paginator.paginate(
            Namespace=METRIC_NAMESPACE,
            MetricName="BytesInPerSec",
            Dimensions=[{"Name": "Cluster Name", "Value": cluster_name}],
        ):
            for metric in page.get("Metrics", []):
                dims = {d["Name"]: d["Value"] for d in metric.get("Dimensions", [])}
                topic = dims.get("Topic")
                broker = dims.get("Broker ID")
                if topic and broker:
                    if topic not in topic_broker_map:
                        topic_broker_map[topic] = set()
                    topic_broker_map[topic].add(broker)
                    broker_ids.add(broker)

        INTERNAL_TOPICS = {"__consumer_offsets", "__amazon_msk_canary", "__amazon_msk_connect"}
        topic_broker_map = {t: b for t, b in topic_broker_map.items() if t not in INTERNAL_TOPICS}

        if not topic_broker_map:
            update_state("topic_metrics", {})
            update_state("topic_metrics_status", "no_topics_found")
            return {}

        topic_metrics = {}

        for topic, brokers in topic_broker_map.items():
            agg_bytes = {}
            agg_msgs = {}
            max_partitions = 0

            for broker_id in brokers:
                full_dims = [
                    {"Name": "Cluster Name", "Value": cluster_name},
                    {"Name": "Broker ID", "Value": broker_id},
                    {"Name": "Topic", "Value": topic},
                ]

                bytes_resp = cw.get_metric_statistics(
                    Namespace=METRIC_NAMESPACE, MetricName="BytesInPerSec",
                    Dimensions=full_dims, StartTime=start_time, EndTime=end_time,
                    Period=period, Statistics=["Average"],
                )
                for dp in bytes_resp.get("Datapoints", []):
                    ts = dp["Timestamp"].isoformat()
                    agg_bytes[ts] = agg_bytes.get(ts, 0) + dp.get("Average", 0)

                msgs_resp = cw.get_metric_statistics(
                    Namespace=METRIC_NAMESPACE, MetricName="MessagesInPerSec",
                    Dimensions=full_dims, StartTime=start_time, EndTime=end_time,
                    Period=period, Statistics=["Average"],
                )
                for dp in msgs_resp.get("Datapoints", []):
                    ts = dp["Timestamp"].isoformat()
                    agg_msgs[ts] = agg_msgs.get(ts, 0) + dp.get("Average", 0)

                part_resp = cw.get_metric_statistics(
                    Namespace=METRIC_NAMESPACE, MetricName="PartitionCount",
                    Dimensions=full_dims, StartTime=start_time, EndTime=end_time,
                    Period=period, Statistics=["Maximum"],
                )
                for dp in part_resp.get("Datapoints", []):
                    val = int(dp.get("Maximum", 0))
                    if val > max_partitions:
                        max_partitions = val

            sorted_ts = sorted(agg_bytes.keys())
            bytes_in_history = [agg_bytes[ts] for ts in sorted_ts]
            bytes_in = bytes_in_history[-1] if bytes_in_history else 0

            sorted_ts_msgs = sorted(agg_msgs.keys())
            msgs_in_history = [agg_msgs[ts] for ts in sorted_ts_msgs]
            msgs_in = msgs_in_history[-1] if msgs_in_history else 0

            avg_bytes = sum(bytes_in_history) / len(bytes_in_history) if bytes_in_history else 0

            latest_bytes = bytes_in_history[-1] if bytes_in_history else 0
            latest_msgs = msgs_in_history[-1] if msgs_in_history else 0
            has_meaningful_activity = (
                latest_bytes > 10 or
                latest_msgs > 0.1 or
                max_partitions > 0
            )

            if not has_meaningful_activity:
                print(f"[TopicMetrics] {topic}: skipped (no recent activity, likely deleted)")
                suppressed = list(get_state().get("suppressed_topics", []))
                if topic in suppressed:
                    suppressed.remove(topic)
                    update_state("suppressed_topics", suppressed)
                continue

            suppressed = list(get_state().get("suppressed_topics", []))
            if topic in suppressed:
                recent_points = [b for b in bytes_in_history[-3:] if b > 10]
                if len(recent_points) >= 2:
                    print(f"[TopicMetrics] {topic}: unsuppressed (new sustained activity detected)")
                    suppressed.remove(topic)
                    update_state("suppressed_topics", suppressed)
                else:
                    print(f"[TopicMetrics] {topic}: skipped (suppressed after clear, CloudWatch data still draining)")
                    continue

            topic_metrics[topic] = {
                "bytes_in_per_sec": bytes_in,
                "messages_in_per_sec": msgs_in,
                "bytes_in_avg": avg_bytes,
                "bytes_in_history": bytes_in_history,
                "msgs_in_history": msgs_in_history,
                "partition_count": max_partitions,
            }

            print(f"[TopicMetrics] {topic}: bytes_in={bytes_in:.1f} B/s, msgs_in={msgs_in:.1f}/s, partitions={max_partitions}")

        sorted_topics = sorted(
            topic_metrics.items(),
            key=lambda x: x[1]["bytes_in_per_sec"],
            reverse=True
        )
        topic_metrics = dict(sorted_topics)

        update_state("topic_metrics", topic_metrics)
        update_state("topic_metrics_updated", end_time.isoformat() + "Z")
        update_state("topic_metrics_status", "ok")

        return topic_metrics

    except Exception as e:
        import traceback
        traceback.print_exc()
        update_state("topic_metrics_status", f"error: {str(e)}")
        return {"error": str(e)}


def get_metric_history_data(metric_name, hours=6):
    return _get_metric_stats(metric_name, hours=hours, period=300)


def get_cloudwatch_alarms():
    try:
        cw = get_client("cloudwatch")
        response = cw.describe_alarms(
            StateValue="ALARM",
            MaxRecords=20
        )
        alarms = []
        for alarm in response.get("MetricAlarms", []):
            alarms.append({
                "name": alarm.get("AlarmName", ""),
                "state": alarm.get("StateValue", ""),
                "metric": alarm.get("MetricName", ""),
                "namespace": alarm.get("Namespace", ""),
                "description": alarm.get("AlarmDescription", ""),
                "threshold": alarm.get("Threshold", 0),
                "updated": str(alarm.get("StateUpdatedTimestamp", "")),
            })
        return alarms
    except Exception as e:
        return {"error": str(e)}


def get_log_events(log_group="/aws/msk/demo-cluster-1", limit=50):
    try:
        logs = get_client("logs")
        response = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=1
        )
        streams = response.get("logStreams", [])
        if not streams:
            return []

        stream_name = streams[0]["logStreamName"]
        events_response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream_name,
            limit=limit,
            startFromHead=False
        )
        events = []
        for event in events_response.get("events", []):
            events.append({
                "timestamp": datetime.fromtimestamp(event["timestamp"] / 1000).isoformat(),
                "message": event.get("message", ""),
            })
        return events
    except Exception as e:
        return {"error": str(e)}
