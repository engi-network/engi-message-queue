import os

from celery import Celery
from kombu.utils.url import safequote

from engi_message_queue import SNSFanoutSQS, get_name

aws_access_key = safequote(os.environ["AWS_ACCESS_KEY_ID"])
aws_secret_key = safequote(os.environ["AWS_SECRET_ACCESS_KEY"])
queue_name_prefix = safequote(get_name())

app = Celery(
    "tasks",
    broker=f"sqs://{aws_access_key}:{aws_secret_key}@",
    broker_transport_options={"queue_name_prefix": f"{queue_name_prefix}-"},
)


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # clean up old status message SNS -> SQS fanouts every five minutes
    sender.add_periodic_task(60.0 * 5, cleanup_old_status_fanouts)


@app.task(bind=True)
def cleanup_old_status_fanouts(self):
    SNSFanoutSQS.cleanup_old("status")
