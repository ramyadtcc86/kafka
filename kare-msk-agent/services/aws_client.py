import boto3
import os
import threading

_clients = {}
_lock = threading.Lock()


def get_aws_session():
    region = os.environ.get("AWS_REGION", "us-east-1")
    kwargs = {"region_name": region}

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")

    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
        if session_token:
            kwargs["aws_session_token"] = session_token

    return boto3.Session(**kwargs)


def get_client(service_name, force_new=False):
    with _lock:
        if service_name not in _clients or force_new:
            session = get_aws_session()
            _clients[service_name] = session.client(service_name)
        return _clients[service_name]


def refresh_clients():
    with _lock:
        _clients.clear()


def get_region():
    return os.environ.get("AWS_REGION", "us-east-1")


def get_cluster_arn():
    return os.environ.get("MSK_CLUSTER_ARN", "")


def credentials_configured():
    return bool(
        os.environ.get("AWS_ACCESS_KEY_ID")
        and os.environ.get("AWS_SECRET_ACCESS_KEY")
        and os.environ.get("MSK_CLUSTER_ARN")
    )
