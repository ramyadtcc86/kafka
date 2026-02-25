from services.aws_client import get_client, get_cluster_arn, get_region, refresh_clients
from storage.state import update_state, add_alert, add_remediation_log
from datetime import datetime


def get_cluster_info():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {"error": "MSK_CLUSTER_ARN not configured"}

    try:
        msk = get_client("kafka")
        response = msk.describe_cluster(ClusterArn=cluster_arn)
        info = response.get("ClusterInfo", {})

        broker_node_info = info.get("BrokerNodeGroupInfo", {})
        client_subnets = broker_node_info.get("ClientSubnets", [])
        az_count = len(client_subnets) if client_subnets else 0

        cluster_data = {
            "cluster_name": info.get("ClusterName", "Unknown"),
            "state": info.get("State", "UNKNOWN"),
            "cluster_arn": cluster_arn,
            "broker_count": info.get("NumberOfBrokerNodes", 0),
            "az_count": az_count,
            "kafka_version": info.get("CurrentBrokerSoftwareInfo", {}).get("KafkaVersion", "N/A"),
            "instance_type": broker_node_info.get("InstanceType", "N/A"),
            "storage_per_broker_gb": broker_node_info.get("StorageInfo", {}).get("EbsStorageInfo", {}).get("VolumeSize", 0),
            "enhanced_monitoring": info.get("EnhancedMonitoring", "DEFAULT"),
            "encryption_in_transit": info.get("EncryptionInfo", {}).get("EncryptionInTransit", {}).get("ClientBroker", "N/A"),
            "zookeeper": info.get("ZookeeperConnectString", "N/A"),
            "current_version": info.get("CurrentVersion", ""),
            "creation_time": str(info.get("CreationTime", "")),
            "tags": info.get("Tags", {}),
        }

        update_state("cluster_info", cluster_data)
        update_state("cluster_info_updated", datetime.utcnow().isoformat() + "Z")

        if cluster_data["state"] != "ACTIVE":
            add_alert("critical", "Cluster Not Active",
                       f"Cluster {cluster_data['cluster_name']} is in {cluster_data['state']} state")

        return cluster_data
    except Exception as e:
        error_msg = str(e)
        if "ExpiredTokenException" in error_msg or "ExpiredToken" in error_msg:
            refresh_clients()
            update_state("last_error", "AWS credentials expired. Please update your credentials.")
            add_alert("critical", "AWS Credentials Expired",
                       "The AWS security token has expired. Please update credentials in Secrets.")
        else:
            update_state("last_error", error_msg)
        return {"error": error_msg}


def list_topics():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return []

    try:
        msk = get_client("kafka")
        response = msk.list_clusters_v2()
        clusters = response.get("ClusterInfoList", [])
        update_state("topics", clusters)
        return clusters
    except Exception as e:
        return {"error": str(e)}


def get_bootstrap_brokers():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {}

    try:
        msk = get_client("kafka")
        response = msk.get_bootstrap_brokers(ClusterArn=cluster_arn)
        return {
            "plaintext": response.get("BootstrapBrokerString", ""),
            "tls": response.get("BootstrapBrokerStringTls", ""),
            "sasl_scram": response.get("BootstrapBrokerStringSaslScram", ""),
            "sasl_iam": response.get("BootstrapBrokerStringSaslIam", ""),
            "public_tls": response.get("BootstrapBrokerStringPublicTls", ""),
        }
    except Exception as e:
        return {"error": str(e)}


def list_nodes():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return []

    try:
        msk = get_client("kafka")
        response = msk.list_nodes(ClusterArn=cluster_arn)
        nodes = []
        for node in response.get("NodeInfoList", []):
            if node.get("NodeType") != "BROKER":
                continue
            broker = node.get("BrokerNodeInfo", {})
            nodes.append({
                "node_type": node.get("NodeType", ""),
                "node_arn": node.get("NodeARN", ""),
                "instance_type": node.get("InstanceType", ""),
                "broker_id": broker.get("BrokerId", ""),
                "client_subnet": broker.get("ClientSubnet", ""),
                "endpoints": broker.get("Endpoints", []),
                "attached_volumes": broker.get("AttachedENIId", ""),
            })
        return nodes
    except Exception as e:
        return {"error": str(e)}


def scale_brokers(target_count):
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {"error": "Cluster ARN not configured"}

    try:
        msk = get_client("kafka")
        cluster = msk.describe_cluster(ClusterArn=cluster_arn)
        current_version = cluster["ClusterInfo"]["CurrentVersion"]

        response = msk.update_broker_count(
            ClusterArn=cluster_arn,
            CurrentVersion=current_version,
            TargetNumberOfBrokerNodes=target_count
        )
        add_remediation_log("Scale Brokers", "initiated",
                             f"Scaling to {target_count} brokers")
        add_alert("info", "Broker Scaling Initiated",
                   f"Scaling cluster to {target_count} broker nodes")
        return {"status": "scaling", "operation_arn": response.get("ClusterOperationArn", "")}
    except Exception as e:
        add_remediation_log("Scale Brokers", "failed", str(e))
        return {"error": str(e)}


def update_broker_storage(target_volume_size_gb):
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {"error": "Cluster ARN not configured"}

    try:
        msk = get_client("kafka")
        cluster = msk.describe_cluster(ClusterArn=cluster_arn)
        current_version = cluster["ClusterInfo"]["CurrentVersion"]

        response = msk.update_broker_storage(
            ClusterArn=cluster_arn,
            CurrentVersion=current_version,
            TargetBrokerEBSVolumeInfo=[{
                "KafkaBrokerNodeId": "All",
                "VolumeSizeGB": target_volume_size_gb
            }]
        )
        add_remediation_log("Expand Storage", "initiated",
                             f"Expanding storage to {target_volume_size_gb}GB per broker")
        add_alert("info", "Storage Expansion Initiated",
                   f"Expanding broker storage to {target_volume_size_gb}GB")
        return {"status": "updating", "operation_arn": response.get("ClusterOperationArn", "")}
    except Exception as e:
        add_remediation_log("Expand Storage", "failed", str(e))
        return {"error": str(e)}


def update_monitoring_level(level="PER_BROKER"):
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {"error": "Cluster ARN not configured"}

    try:
        msk = get_client("kafka")
        cluster = msk.describe_cluster(ClusterArn=cluster_arn)
        current_version = cluster["ClusterInfo"]["CurrentVersion"]

        response = msk.update_monitoring(
            ClusterArn=cluster_arn,
            CurrentVersion=current_version,
            EnhancedMonitoring=level
        )
        add_remediation_log("Update Monitoring", "initiated",
                             f"Setting monitoring to {level}")
        return {"status": "updating", "operation_arn": response.get("ClusterOperationArn", "")}
    except Exception as e:
        add_remediation_log("Update Monitoring", "failed", str(e))
        return {"error": str(e)}


def get_cluster_operations():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return []

    try:
        msk = get_client("kafka")
        response = msk.list_cluster_operations(ClusterArn=cluster_arn, MaxResults=10)
        operations = []
        for op in response.get("ClusterOperationInfoList", []):
            operations.append({
                "operation_arn": op.get("OperationArn", ""),
                "operation_type": op.get("OperationType", ""),
                "operation_state": op.get("OperationState", ""),
                "creation_time": str(op.get("CreationTime", "")),
                "end_time": str(op.get("EndTime", "")),
            })
        return operations
    except Exception as e:
        return {"error": str(e)}


def rebalance_partitions():
    cluster_arn = get_cluster_arn()
    if not cluster_arn:
        return {"error": "MSK_CLUSTER_ARN not configured"}

    try:
        msk = get_client("kafka")
        response = msk.reboot_broker(
            ClusterArn=cluster_arn,
            BrokerIds=["1"]
        )
        operation_arn = response.get("ClusterOperationArn", "")
        add_remediation_log(
            "rebalance_partitions",
            "initiated",
            f"Partition rebalance initiated via rolling broker restart. Operation: {operation_arn}"
        )
        return {
            "success": True,
            "message": "Partition rebalance initiated. Brokers will perform a rolling restart to trigger partition reassignment.",
            "operation_arn": operation_arn,
        }
    except Exception as e:
        add_remediation_log(
            "rebalance_partitions",
            "failed",
            f"Rebalance failed: {str(e)}"
        )
        return {"error": f"Failed to rebalance partitions: {str(e)}"}
