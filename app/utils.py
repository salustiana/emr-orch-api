import os
from collections import namedtuple

from melitk import logging

logger = logging.getLogger("cluster_manager")


DATADOG_METRIC_NAME = "business.emr_clusters_manager.{}"
DatadogMetric = namedtuple("DatadogMetric", "metric_name tags")


def datadog_metric(metric_name: str, metric_tags: dict) -> DatadogMetric:
    formatted_metric = DATADOG_METRIC_NAME.format(metric_name)
    datadog_metric = DatadogMetric(metric_name=formatted_metric, tags=metric_tags)
    return datadog_metric


def is_prod() -> bool:
    return os.getenv("SCOPE")
