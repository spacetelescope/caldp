import os


def get_environment():
    queue_name = os.environ.get("AWS_BATCH_JQ_NAME", "")
    environment = queue_name.rsplit("-", 1)[-1] if queue_name else None
    return environment
