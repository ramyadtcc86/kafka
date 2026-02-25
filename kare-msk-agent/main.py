import boto3
import os
import time
from replit import ai

# To use this, you need to set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION in Secrets.
# Also provide the MSK Cluster ARN in MSK_CLUSTER_ARN secret.

def get_msk_status():
    cluster_arn = os.environ.get("MSK_CLUSTER_ARN")
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    if not cluster_arn:
        return "Error: MSK_CLUSTER_ARN environment variable not set."
    
    try:
        msk = boto3.client("kafka", region_name=region)
        
        # Add AWS_SESSION_TOKEN if available in environment
        session_token = os.environ.get("AWS_SESSION_TOKEN")
        if session_token:
            msk = boto3.client(
                "kafka", 
                region_name=region,
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                aws_session_token=session_token
            )
            
        response = msk.describe_cluster(ClusterArn=cluster_arn)
        cluster_info = response.get("ClusterInfo", {})
        
        status = cluster_info.get("State", "UNKNOWN")
        node_info = cluster_info.get("NumberOfBrokerNodes", 0)
        zookeeper = cluster_info.get("ZookeeperConnectString", "N/A")
        
        return {
            "status": status,
            "broker_nodes": node_info,
            "zookeeper": zookeeper,
            "cluster_name": cluster_info.get("ClusterName")
        }
    except Exception as e:
        return f"Error fetching MSK status: {str(e)}"

def create_cloudwatch_log_group():
    region = os.environ.get("AWS_REGION", "us-east-1")
    log_group_name = "/aws/msk/demo-cluster-1"
    
    try:
        logs = boto3.client("logs", region_name=region)
        
        # Add AWS_SESSION_TOKEN if available in environment
        session_token = os.environ.get("AWS_SESSION_TOKEN")
        if session_token:
            logs = boto3.client(
                "logs", 
                region_name=region,
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                aws_session_token=session_token
            )
            
        # Check if log group exists
        try:
            logs.create_log_group(logGroupName=log_group_name)
            print(f"Created CloudWatch Log Group: {log_group_name}")
        except logs.exceptions.ResourceAlreadyExistsException:
            print(f"CloudWatch Log Group already exists: {log_group_name}")
            
        # Create log stream
        log_stream_name = "msk-broker-logs"
        try:
            logs.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
            print(f"Created CloudWatch Log Stream: {log_stream_name}")
        except logs.exceptions.ResourceAlreadyExistsException:
            print(f"CloudWatch Log Stream already exists: {log_stream_name}")
            
        return log_group_name
    except Exception as e:
        print(f"Error creating CloudWatch resources: {str(e)}")
        return None

def update_msk_logging(log_group_name):
    cluster_arn = os.environ.get("MSK_CLUSTER_ARN")
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    if not cluster_arn or not log_group_name:
        return
        
    try:
        msk = boto3.client("kafka", region_name=region)
        
        # Add AWS_SESSION_TOKEN if available in environment
        session_token = os.environ.get("AWS_SESSION_TOKEN")
        if session_token:
            msk = boto3.client(
                "kafka", 
                region_name=region,
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                aws_session_token=session_token
            )
            
        # Describe cluster to get current version
        cluster = msk.describe_cluster(ClusterArn=cluster_arn)
        current_version = cluster["ClusterInfo"]["CurrentVersion"]
        
        msk.update_monitoring(
            ClusterArn=cluster_arn,
            CurrentVersion=current_version,
            LoggingInfo={
                'BrokerLogs': {
                    'CloudWatchLogs': {
                        'Enabled': True,
                        'LogGroup': log_group_name
                    }
                }
            }
        )
        print(f"Enabled CloudWatch logging for MSK cluster to {log_group_name}")
    except Exception as e:
        print(f"Error updating MSK logging: {str(e)}")

def agent_monitor():
    print("AI Agent: Starting MSK monitoring...")
    
    # Initialize CloudWatch logging
    log_group = create_cloudwatch_log_group()
    logging_updated = False
    
    # Simple monitoring loop
    while True:
        status_data = get_msk_status()
        
        if isinstance(status_data, str):
            print(status_data)
        else:
            print(f"[{time.ctime()}] Cluster: {status_data['cluster_name']} | Status: {status_data['status']} | Brokers: {status_data['broker_nodes']}")
            
            # Enable logging once the cluster is ACTIVE
            if not logging_updated and status_data['status'] == 'ACTIVE' and log_group:
                update_msk_logging(log_group)
                logging_updated = True
            
            # Here we could use Replit AI to analyze the status if it was more complex
            # For a simple "print status", we just print it.
            
            if status_data['status'] != 'ACTIVE':
                print(f"ALERT: Cluster is in {status_data['status']} state!")
        
        # Wait for 60 seconds before next check
        time.sleep(60)

if __name__ == "__main__":
    # Check for credentials
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in Secrets.")
    else:
        agent_monitor()
