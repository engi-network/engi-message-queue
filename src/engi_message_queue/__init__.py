import json
import logging
import os
from datetime import datetime

import boto3
import coloredlogs

sns_client = boto3.client("sns")
sqs_client = boto3.client("sqs")
sts_client = boto3.client("sts")

AWS_REGION = boto3.session.Session().region_name
AWS_ACCOUNT = sts_client.get_caller_identity()["Account"]
ENGI_MESSAGE_QUEUE_APP_NAME = os.environ["ENGI_MESSAGE_QUEUE_APP_NAME"]


def get_name(env=None):
    if env is None:
        env = os.environ.get("ENV", "dev")
    return f"{ENGI_MESSAGE_QUEUE_APP_NAME}-{env}"


def setup_logging(log_level=logging.INFO):
    logger = logging.getLogger()

    # set log format to display the logger name to hunt down verbose logging modules
    fmt = "%(asctime)s %(name)-25s %(levelname)-8s %(message)s"

    coloredlogs.install(level=log_level, fmt=fmt, logger=logger)

    return logger


log = setup_logging()


def get_sns_arn(name):
    return f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT}:{name}"


def get_sqs_url(name):
    return f"{sqs_client.meta._endpoint_url}/{AWS_ACCOUNT}/{name}"


def get_topic_arn(queue_url):
    return get_sns_arn(queue_url.split("/")[-1])


def allow_all_to_publish_to_sns(topic_arn):
    return """{{
        "Id": "Policy1654105353800",
        "Version": "2012-10-17",
        "Statement": [
            {{
            "Sid": "Stmt1654105351953",
            "Action": [
                "sns:Publish"
            ],
            "Effect": "Allow",
            "Resource": "{}",
            "Principal": "*"
            }}
        ]
        }}""".format(
        topic_arn
    )


def allow_sns_to_write_to_sqs(topic_arn, queue_arn):
    return """{{
        "Version":"2012-10-17",
        "Statement":[
            {{
            "Sid": "MyPolicy",
            "Effect": "Allow",
            "Principal": {{"AWS" : "*"}},
            "Action": "SQS:SendMessage",
            "Resource": "{}",
            "Condition":{{
                "ArnEquals":{{
                "aws:SourceArn": "{}"
                }}
            }}
            }}
        ]
        }}""".format(
        queue_arn, topic_arn
    )


class NullFanout(object):
    def __init__(self, *_, **__):
        self.topic_arn = None

    def __enter__(self):
        return self

    def receive(self, **_):
        yield

    def publish(self, *_, **__):
        pass

    def __exit__(self, *_):
        pass


class SNSFanoutSQS(object):
    """Create a SNS topic and connect it to an SQS queue. Use contextlib to
    optionally tear down both the topic and queue after exiting a with
    statement"""

    def __init__(self, name, visibility_timeout=180, persist=False, fifo=True):
        self.sns = sns_client
        self.sqs = sqs_client
        self.name = f"{name}.fifo" if fifo else name
        self.visibility_timeout = visibility_timeout
        self.persist = persist
        self.fifo = fifo
        self.created = False

    @staticmethod
    def load(queue_url):
        self = SNSFanoutSQS("")
        self.queue_url = queue_url
        self.topic_arn = get_topic_arn(queue_url)
        return self

    @staticmethod
    def cleanup_old(suffix, age_cutoff=60 * 60):
        now = datetime.utcnow()
        log.info("cleaning up old SQS and SNS queues")
        r = sqs_client.list_queues(QueueNamePrefix=get_name())
        for queue_url in r["QueueUrls"]:
            if not suffix in queue_url:
                continue
            try:
                r_ = sqs_client.get_queue_attributes(
                    QueueUrl=queue_url, AttributeNames=["LastModifiedTimestamp"]
                )
                age = (
                    now - datetime.utcfromtimestamp(int(r_["Attributes"]["LastModifiedTimestamp"]))
                ).seconds
                log.info(f"{queue_url} is {age} seconds old, {age_cutoff=}")
                if age >= age_cutoff:
                    SNSFanoutSQS.load(queue_url).cleanup()
            except sqs_client.exceptions.QueueDoesNotExist:
                continue

    def topic_exists(self):
        return getattr(self, "topic_arn", get_sns_arn(self.name)) in [
            t["TopicArn"] for t in self.sns.list_topics()["Topics"]
        ]

    def create(self):
        if self.topic_exists():
            log.info(f"topic {self.name} exists")
            self.topic_arn = get_sns_arn(self.name)
            self.queue_url = get_sqs_url(self.name)
            return self
        log.info(f"creating {self.name=}")
        # create the SQS queue
        attrs = {"VisibilityTimeout": str(self.visibility_timeout)}
        if self.fifo:
            attrs["FifoQueue"] = "true"
        r = self.sqs.create_queue(
            QueueName=self.name,
            Attributes=attrs,
        )
        self.queue_url = r["QueueUrl"]
        r = self.sqs.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["QueueArn"])
        self.queue_arn = r["Attributes"]["QueueArn"]
        log.info(f"{self.queue_url=} {self.queue_arn=}")
        log.info(f"creating {self.name=}")
        # create the SNS topic
        r = self.sns.create_topic(
            Name=self.name,
            Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"}
            if self.fifo
            else {},
        )
        self.topic_arn = r["TopicArn"]
        log.info(f"{self.topic_arn=}")
        # subscribe the topic to the queue, aka fanout
        r = self.sns.subscribe(
            TopicArn=self.topic_arn,
            Protocol="sqs",
            Endpoint=self.queue_arn,
            ReturnSubscriptionArn=True,
        )
        # permissions
        r = self.sns.set_topic_attributes(
            TopicArn=self.topic_arn,
            AttributeName="Policy",
            AttributeValue=allow_all_to_publish_to_sns(self.topic_arn),
        )
        r = self.sqs.set_queue_attributes(
            QueueUrl=self.queue_url,
            Attributes={"Policy": allow_sns_to_write_to_sqs(self.topic_arn, self.queue_arn)},
        )
        self.created = True
        return self

    def last_modified(self):
        r = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url, AttributeNames=["LastModifiedTimestamp"]
        )
        return datetime.utcfromtimestamp(int(r["Attributes"]["LastModifiedTimestamp"]))

    def __enter__(self):
        return self.create()

    def publish(self, d, **kwargs):
        return self.sns.publish(TopicArn=self.topic_arn, Message=json.dumps(d), **kwargs)

    def delete_message(self, receipt_handle):
        return self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )

    def receive(self, wait_time=5, max_messages=1, delete=True):
        r = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
        )
        for m in r.get("Messages", []):
            msg = json.loads(json.loads(m["Body"])["Message"])
            receipt_handle = m["ReceiptHandle"]
            # log.info(f"{receipt_handle=}")
            if delete:
                yield msg
                self.delete_message(receipt_handle)
            else:
                yield msg, receipt_handle

    def cleanup(self):
        if self.topic_exists():
            log.info(f"deleting {self.queue_url=} {self.topic_arn=}")
            self.sqs.delete_queue(QueueUrl=self.queue_url)
            self.sns.delete_topic(TopicArn=self.topic_arn)
            return True
        return False

    def __exit__(self, *_):
        if not self.persist:
            self.cleanup()
