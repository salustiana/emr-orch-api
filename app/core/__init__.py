import time
from datetime import datetime
from threading import Thread
from typing import List
from collections import namedtuple

from app.core.models import Cluster, StepsCluster, Step
from app.core.errors import (
    UpdateStatusError,
    UnableToTerminateClusterError,
    CreateClusterError,
    UnableToAssignStepError,
)
from app.extensions import db
from app.utils import logger

from flask import current_app
from sqlalchemy import and_
from melitk import metrics
from melitk.metrics.exceptions import MetricsError


class FlaskThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app = current_app._get_current_object()

    def run(self):
        with self.app.app_context():
            super().run()


"""
Based on: https://docs.aws.amazon.com/general/latest/gr/emr.html
There is a common issue when using some AWS APIs. Whenever you exceed some requests
frequency, a `ThrottlingException` is raised. To avoid it, we'll set some limits
and sleeps.
"""

emr_quota_bucket = namedtuple("emr_quota_bucket", "maximum_capacity refill_rate")

FREQUENCY_LIMIT_COEFFICIENT = 1

# Following constants represent max request per second

MAX_RUN_JOB_FLOW_FREQUENCY = emr_quota_bucket(
    10 * FREQUENCY_LIMIT_COEFFICIENT, 0.5
)  # To create clusters
MAX_ADD_JOB_FLOW_STEPS_FREQUENCY = emr_quota_bucket(
    10 * FREQUENCY_LIMIT_COEFFICIENT, 0.5
)  # To add steps
MAX_DESCRIBE_CLUSTER_FREQUENCY = emr_quota_bucket(
    10 * FREQUENCY_LIMIT_COEFFICIENT, 1.0
)  # To retrieve clusters status
MAX_TERMINATE_JOB_FLOWS_FREQUENCY = emr_quota_bucket(
    10 * FREQUENCY_LIMIT_COEFFICIENT, 0.5
)  # To terminate clusters
MAX_CANCEL_STEPS_FREQUENCY = emr_quota_bucket(
    10 * FREQUENCY_LIMIT_COEFFICIENT, 0.2
)  # To cancel steps. TODO: not listed in the docs
MAX_DESCRIBE_STEP_FREQUENCY = emr_quota_bucket(
    10 * FREQUENCY_LIMIT_COEFFICIENT, 0.5
)  # To retrieve step status


def update_clusters():
    """Updates cluster's status prior to define which steps should be launched."""

    clusters_to_update = Cluster.query.filter(
        ~Cluster.status.in_(Cluster.TERMINATED_STATUS)
    ).all()

    clusters_to_update += StepsCluster.query.filter(
        ~StepsCluster.status.in_(Cluster.TERMINATED_STATUS)
    ).all()

    for index, cluster in enumerate(clusters_to_update):
        try:
            logger.debug("Updating cluster {}".format(cluster.id))
            cluster.update_status()
            logger.debug("Updated cluster {}".format(cluster.id))
            sleep_for_service_quota(MAX_DESCRIBE_CLUSTER_FREQUENCY, index)
        except UpdateStatusError:
            pass

    db.session.commit()


def update_steps():
    """Updates steps's status prior to define which steps should be launched."""
    steps_to_update = Step.query.filter(~Step.status.in_(Step.TERMINATED_STATUS))
    for index, step in enumerate(steps_to_update):
        try:
            logger.debug("Updating step {}".format(step.id))
            step.update_status()
            logger.debug("Updated step {}".format(step.id))
            sleep_for_service_quota(MAX_DESCRIBE_STEP_FREQUENCY, index)
        except UpdateStatusError:
            pass

    db.session.commit()


def sleep_for_service_quota(
    max_frequency: emr_quota_bucket, bucket_capacity_usage: int
):
    """
    Function to calculate the sleep time that should apply according to the `max_frequency` of each
    AWS function and its `max_frequency`
    Args:
        max_frequency (emr_quota_bucket): max frequency for the used AWS function
        bucket_capacity_usage (int): number of request to the AWS service
    Returns:
        float: sleep time required to avoid a `ThrottlingException`
    """
    if bucket_capacity_usage < max_frequency.maximum_capacity:
        return 0

    sleep_time = float(1 / max_frequency.refill_rate) * FREQUENCY_LIMIT_COEFFICIENT
    time.sleep(sleep_time)


class Manager:
    def __init__(self, step_ids: List[int] = None):
        # Step IDs to be managed
        self.step_ids = step_ids

    def __enter__(self):
        t_update_clusters = FlaskThread(target=update_clusters)
        t_update_steps = FlaskThread(target=update_steps)

        t_update_clusters.start()
        t_update_steps.start()

        t_update_clusters.join()
        t_update_steps.join()

        self.unassigned_steps = self._unassigned_steps().with_for_update().all()

        # If with_for_update() is added to waiting_step_clusters,
        # many clusters are instantiated for the same step.
        self.waiting_step_clusters = StepsCluster.query.filter_by(
            status=Cluster.UNASSIGNED_STATUS
        ).all()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _unassigned_steps(self):
        unassigned_steps = Step.query.filter_by(status=Step.UNASSIGNED_STATUS)
        if self.step_ids:
            unassigned_steps = unassigned_steps.filter(Step.id.in_(self.step_ids))

        return unassigned_steps

    def run(self):
        categorized_clusters = self.categorize_clusters()

        for index, step in enumerate(self.unassigned_steps):
            try:
                assigned_cluster = self.assign_step_to_cluster(
                    step, categorized_clusters
                )
                sleep_for_service_quota(MAX_ADD_JOB_FLOW_STEPS_FREQUENCY, index)

            except (CreateClusterError, UnableToAssignStepError) as e:
                # CreateClusterError: This error is raised if the job_flow_config provided is rejected by aws.
                # UnableToAssignStepError: This error is raised if AWS is unable to insert the step into a cluster.
                logger.error(e)
                step.status = "BAD_CONFIG"

            except Exception as e:
                # Catch all exceptions when inserting so that BigQ doesn't endlessly POST to /manage.
                logger.error(e)
                step.status = "ERROR"

        self.terminate_clusters()

        db.session.commit()

    def categorize_clusters(self) -> dict:
        """
        Categorizes available clusters given their settings.

        If will be used to assign steps that require a configuration to a cluster (if it
        exists), or to define that a new cluster should be created.

        Returns:
            dict. a dictionary where each key is a Cluster's configuration hash value, and the value
        is the list of clusters available with that same configuration.
        """
        categorized_clusters = {}
        for cluster in self.waiting_step_clusters:
            try:
                matching_clusters = categorized_clusters[hash(cluster)]
                matching_clusters.append(cluster)
                categorized_clusters[hash(cluster)] = matching_clusters
            except KeyError:
                categorized_clusters[hash(cluster)] = [cluster]

        return categorized_clusters

    def assign_step_to_cluster(self, step, categorized_clusters: dict) -> StepsCluster:
        """
        Adds the `step` to a cluster, either an existent one or to a newly created one.

        Args:
            step (Step): step to be assigned.
            categorized_clusters (dict): available clusters grouped by requirements.

        Returns:
            StepsCluster: assigned to the input step.

        """
        viable_cluster = self.get_viable_cluster(step, categorized_clusters)

        cluster_id, step_id = viable_cluster.add_step(step)
        step.check_in(cluster_id, step_id)

        datadog_metric = viable_cluster.metrics()
        try:
            metrics.record_count(
                name=datadog_metric.metric_name, increment=1, tags=datadog_metric.tags
            )
        except MetricsError as e:
            logger.info(
                "Unable to post {} metric with tags {} - msg: {}".format(
                    datadog_metric.metric_name, datadog_metric.tags, e
                )
            )

        db.session.add(viable_cluster)
        db.session.add(step)
        db.session.commit()

        return viable_cluster

    def get_viable_cluster(self, step, categorized_clusters: dict) -> StepsCluster:
        """
        Receives a step and available clusters grouped by configuration and chooses the
        right cluster for the input step. If there is no available cluster for the required
        step, then creates one.

        Args:
            step (Step): step to be executed.
            categorized_clusters (dict): output from the `categorize_clusters_by_hash` method of the current class.
                Revisit the method for further documentation.

        Returns:
            StepsCluster: to be used to execute the step.

        """
        viable_clusters = categorized_clusters.get(hash(step))
        if viable_clusters:
            return viable_clusters.pop(0)

        cluster = StepsCluster(
            credentials=step.credentials, job_flow_config=step.job_flow_config
        )
        cluster.create()
        db.session.commit()
        return cluster

    def terminate_clusters(self) -> None:
        """
        Terminates received clusters.

        Raises:
            Passes exception to avoid stopping the flow for single errors.

        """
        for cluster in self.expired_clusters():
            try:
                cluster.terminate()
            except UnableToTerminateClusterError:
                pass

    def expired_clusters(self) -> List[StepsCluster]:
        """Fetches `expired` clusters. Clusters are considered expired when
        they have a `terminate_on` date older than utcnow()."""

        now_date = datetime.utcnow()

        expired_step_clusters = (
            StepsCluster.query.filter(
                and_(
                    ~StepsCluster.status.in_(StepsCluster.TERMINATED_STATUS),
                    StepsCluster.terminate_on != None,
                    # read this as: terminate_on is older than now.
                    StepsCluster.terminate_on < now_date,
                )
            )
            .with_for_update()
            .all()
        )

        expired_clusters = (
            Cluster.query.filter(
                and_(
                    ~Cluster.status.in_(Cluster.TERMINATED_STATUS),
                    Cluster.terminate_on != None,
                    # read this as: terminate_on is older than now.
                    Cluster.terminate_on < now_date,
                )
            )
            .with_for_update()
            .all()
        )

        return expired_step_clusters + expired_clusters
