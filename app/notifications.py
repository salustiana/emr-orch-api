from app.utils import logger, is_prod

from melitk.bigqueue import Producer
from melitk.melipass import get_env


class LocalProducer(Producer):
    """Class to mock Producer used in production for local testing."""

    def publish(self, message: dict, filters=None, delivery_time=None):
        logger.info("Publishing: {}".format(message))


def producer():
    base_url = get_env(
        "BIGQUEUE_TOPIC_CLUSTER_MANAGER_STEPS_ENDPOINT", "local_topic_endpoint"
    )
    topic = get_env(
        "BIGQUEUE_TOPIC_CLUSTER_MANAGER_STEPS_TOPIC_NAME", "local_topic_name"
    )

    logger.info("Starting with base_url {} and topic {}".format(base_url, topic))

    if is_prod():
        return Producer(
            base_url=base_url, topic=topic, logger=None, timeout=10
        )  # Setting a high timeout because step's management can be slow.

    return LocalProducer(base_url=base_url, topic=topic, logger=None)


steps_producer = producer()
