import json
import os
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import wraps

from app.core.aws_handler import S3Handler, EMRHandler
from app.core.errors import (
    CreateClusterError,
    StepCreationError,
    UnableToTerminateClusterError,
    UnableToAssignStepError,
    UpdateStatusError,
    StepCancelError,
    UnableToUploadContentError,
)
from app.database import Encrypted
from app.extensions import db
from app.utils import datadog_metric, logger

from sqlalchemy import orm
from flask_login import current_user


def status_handler(on_error, on_success=None):
    """
    Handles the update of an object's status based on the function result (success or Exception)

    After noticing that AWS APIs have a delay on updating resources, we decided to manually set
    models's status manually trying to replicate AWS status.

    Obs.: do not add transactional logic in this decorator (such as commits, rollbacks, etc). Those cases
        should be handled by the decorated methods.

    Args:
         on_error (str): Status to set when the functions terminates with an exception
         on_success (str): Status to set when the functions terminates successfully
    """

    def decorator(function):
        @wraps(function)
        def wrapper(self, *args, **kwargs):
            try:
                retval = function(self, *args, **kwargs)
            except Exception as e:
                logger.error(e)
                if "ExpiredTokenException" in str(e):
                    self.status = "EXPIRED_TOKEN"
                else:
                    self.status = on_error
                raise
            else:
                # In some cases, only errors must be catched; therefore,
                # status is not modified
                if on_success:
                    self.status = on_success

            return retval

        return wrapper

    return decorator


class Base(db.Model):
    __abstract__ = True

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class Step(Base):
    """
    Taken from:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html

    Step Status:
    'State': 'PENDING'|'CANCEL_PENDING'|'RUNNING'|'COMPLETED'|'CANCELLED'|'FAILED'|'INTERRUPTED'|'NO_UPDATE',

    Status are represented the same way they figure in EMR, except for the first Status: `UNASSIGNED_STATUS`,
    which represent a Step that has been submitted to the API but was not assigned to a Cluster.

    """

    __tablename__ = "steps"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), nullable=False)
    step_id = db.Column(db.String(64), nullable=True)
    status = db.Column(db.String(64), nullable=False)
    cluster_id = db.Column(db.String(64), nullable=True)
    custom_metadata = db.Column(db.JSON, nullable=False)
    user = db.Column(db.String(64), nullable=False)
    credentials = db.Column(Encrypted, nullable=False)
    is_test = db.Column(db.Boolean, nullable=False)

    step_config = db.Column(db.JSON, nullable=False)
    job_flow_config = db.Column(db.JSON, nullable=True)
    properties_snapshot = db.Column(db.JSON, default={}, nullable=False)

    TERMINATED_STATUS = [
        "COMPLETED",
        "CANCELLED",
        "FAILED",
        "INTERRUPTED",
        "BAD_CONFIG",
        "ERROR",
        "EXPIRED_TOKEN",
    ]
    UNASSIGNED_STATUS = "UNASSIGNED"

    @status_handler(on_error="FAILED", on_success="UNASSIGNED")
    def __init__(
        self,
        custom_metadata: dict,
        credentials: dict,
        step_config: dict,
        job_flow_config: dict,
        is_test: bool = False,
    ):
        try:
            self.step_config = step_config
            self.name = self.step_config["Name"]
            self.custom_metadata = custom_metadata
            self.is_test = is_test
            self.credentials = credentials
            self.job_flow_config = OrderedDict(job_flow_config)
            self.emr_handler = EMRHandler(**self.credentials["emr"])
            self.s3_handler = S3Handler(**self.credentials["s3"])

            self.emr_handler.check_permissions()
            self.user = current_user.get_id()

            # Retrieved from AWS after step insertion
            self.step_id = None
            self.logs_uri = None
            self.created_on = None
            self.started_on = None
            self.ended_on = None

        except Exception as e:
            raise StepCreationError(
                "Error creating the requested step - msg: {}".format(e)
            )

    def __hash__(self):
        hash = int(
            "00".join([str(ord(elem)) for elem in json.dumps(self.job_flow_config)])
        )
        return hash

    @orm.reconstructor
    def init_on_load(self):
        # CAREFUL. Attributes that are not persisted
        # in the DB can only be accessed after the
        # reconstructor method has instantiated them.

        self.s3_handler = S3Handler(**self.credentials["s3"])
        self.emr_handler = EMRHandler(**self.credentials["emr"])

        if self.cluster_id:
            self.get_logs_uri()
        self.get_timeline()

    def get_timeline(self):
        try:
            timeline = self.properties_snapshot["Step"]["Status"]["Timeline"]
        except KeyError:
            return

        self.created_on = timeline.get("CreationDateTime")
        self.started_on = timeline.get("StartDateTime")
        self.ended_on = timeline.get("EndDateTime")

    def get_logs_uri(self):
        try:
            cluster = StepsCluster.query.get(self.cluster_id)
            if not cluster:
                cluster = Cluster.query.get(self.cluster_id)
            if not cluster:
                raise UpdateStatusError(
                    "The cluster {}, to which step {} was assigned, was not found in the DB".format(
                        self.cluster_id, self.id
                    )
                )
        except Exception as e:
            logger.error("Unable to get log URI - msg: {}".format(e))
        else:
            if cluster.logs_uri:
                self.logs_uri = os.path.join(cluster.logs_uri, "steps", self.step_id)

    @status_handler(on_error="NO_UPDATE")
    def update_status(self):
        """
        Updates a Step's status based on EMR status.

        Raises:
            UpdateStatusError: when the status can't be retrieved from EMR.

        """
        if self.cluster_id and self.step_id:
            try:
                response = self.emr_handler.describe_step(
                    cluster_id=self.cluster_id, step_id=self.step_id
                )
                self.properties_snapshot = json.loads(json.dumps(response, default=str))
            except UpdateStatusError as e:
                logger.error(
                    "Error updating status for step {} - msg: {}".format(
                        self.step_id, e
                    )
                )
                raise

            self.status = response["Step"]["Status"]["State"]

    @status_handler(on_error="FAILED", on_success="PENDING")
    def check_in(self, cluster_id: str, step_id: str) -> None:
        """Steps are added to Clusters in EMR. This method checks in the step in the application."""
        logger.info("Added step {} to cluster {}".format(step_id, cluster_id))
        self.cluster_id = cluster_id
        self.step_id = step_id

    @status_handler(on_error="CANCEL_ERROR", on_success="CANCELLED")
    def cancel(self):
        """
        Uses 'SEND_INTERRUPT' to signal the cluster where this step is running to remove
        this step from its queue.

        """

        if self.cluster_id:
            cluster = StepsCluster.query.get(self.cluster_id)
            if not cluster.is_terminated():
                try:
                    self.emr_handler.cancel_steps(
                        cluster_id=self.cluster_id, step_id=self.step_id
                    )
                except StepCancelError as e:
                    logger.error(
                        "Error cancelling step {} in cluster {} - msg: {}".format(
                            self.id, self.cluster_id, e
                        )
                    )

        logger.info(
            "Terminating step {} in cluster {}".format(self.id, self.cluster_id)
        )

    def metrics(self) -> (str, dict):
        return datadog_metric(
            self.__class__.__name__.lower(),
            {
                "name": self.name,
                "user": self.user,
                "is_test": self.is_test,
                "cluster_config_name": self.job_flow_config.get("Name"),
                "custom_metadata": self.custom_metadata,
            },
        )


class Cluster(Base):

    """
    Taken from:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html

    Cluster Status:
    'State': 'STARTING'|'BOOTSTRAPPING'|'RUNNING'|'WAITING'|'TERMINATING'|'TERMINATED'|'TERMINATED_WITH_ERRORS'|'NO_UPDATE',

    Status are represented the same way they figure in EMR.

    """

    __tablename__ = "clusters"

    id = db.Column(db.String(64), primary_key=True)
    status = db.Column(db.String(64), nullable=False)
    terminate_on = db.Column(db.DateTime)
    assigned_steps = db.Column(db.PickleType)
    user = db.Column(db.String(64), nullable=False)
    credentials = db.Column(Encrypted, nullable=False)
    job_flow_config = db.Column(db.JSON, nullable=False)
    properties_snapshot = db.Column(db.JSON, default={}, nullable=False)

    TERMINATED_STATUS = ["TERMINATED", "TERMINATED_WITH_ERRORS", "EXPIRED_TOKEN"]
    UNASSIGNED_STATUS = "WAITING"

    @status_handler(on_error="TERMINATED", on_success="STARTING")
    def __init__(self, credentials: dict, job_flow_config: dict, lifetime: int = 240):
        self.credentials = credentials
        self.job_flow_config = OrderedDict(job_flow_config)
        self.emr_handler = EMRHandler(**self.credentials["emr"])
        self.s3_handler = S3Handler(**self.credentials["s3"])
        self.assigned_steps = []

        self.user = current_user.get_id()
        self.terminate_on = datetime.utcnow() + timedelta(minutes=lifetime)

        # Retrieved from AWS after initialization
        self.id = None
        self.status = None
        self.ip_address = None
        self.logs_uri = None
        self.created_on = None
        self.ready_on = None
        self.ended_on = None

    def __hash__(self):
        hash = int(
            "00".join([str(ord(elem)) for elem in json.dumps(self.job_flow_config)])
        )
        return hash

    @orm.reconstructor
    def init_on_load(self):
        # CAREFUL. Attributes that are not persisted
        # in the DB can only be accessed after the
        # reconstructor method has instantiated them.
        self.s3_handler = S3Handler(**self.credentials["s3"])
        self.emr_handler = EMRHandler(**self.credentials["emr"])

        if self.properties_snapshot:
            self.ip_address = self._master_dnsname_to_ip()
            logs_uri = self.properties_snapshot.get("Cluster").get("LogUri")
            self.logs_uri = os.path.join(logs_uri, self.id)
            self.tags = self.properties_snapshot.get("Cluster").get("Tags")
            self.get_timeline()
        else:
            self.ip_address = None
            self.logs_uri = None
            self.tags = None

    def _master_dnsname_to_ip(self):
        """
        Formats an EMR Cluster's dns name into an IP address.

        Master DNS Name format: "ip-10-63-57-26.ec2.internal"
        Output IP: "10.63.57.26"
        """
        master_dnsname = self.properties_snapshot.get("Cluster").get(
            "MasterPublicDnsName"
        )
        if master_dnsname:
            ip = master_dnsname.split(".")[0]
            ip = ".".join(ip.split("-")[1:])
            return ip

    def get_timeline(self):
        try:
            timeline = self.properties_snapshot["Cluster"]["Status"]["Timeline"]
        except KeyError:
            return

        self.created_on = timeline.get("CreationDateTime")
        self.ready_on = timeline.get("ReadyDateTime")
        self.ended_on = timeline.get("EndDateTime")

    @status_handler(on_error="ERROR", on_success="STARTING")
    def create(self):
        """
        Creates a cluster in AWS and sets the cluster's ID.

        Raises:
            CreateClusterError: when the cluster can't be created in EMR.
            ParamValidationError: when the config provided in the request is rejected by aws.

        """
        try:
            self.id = self.emr_handler.create_cluster(self.job_flow_config)
            logger.info(f"Created cluster {self.id}")
        except CreateClusterError as e:
            logger.error("Error creating cluster {} - msg: {}".format(self.id, e))
            raise

    @status_handler(on_error="ERROR")
    def add_step(self, step) -> (str, str):
        """
        Adds a step to an existing cluster

        Args:
            step: a step submitted to the application.

        Returns:
            (str, str):
                -  `cluster_id` where the step was checked-in
                - `step_id` in EMR of the added step

        Raises:
            UnableToAssignStepError: when step cannot be assigned.

        """
        logger.info("Adding a step to cluster {}".format(self.id))
        try:
            inserted_step_id = self.emr_handler.add_step_to_cluster(
                cluster_id=self.id, step=step
            )

        except UnableToAssignStepError as e:
            logger.error(
                "Error adding step {} to cluster {} - msg {}".format(
                    step.id, self.id, e
                )
            )
            raise

        else:
            self.assigned_steps.append(inserted_step_id)
            self.terminate_on = None

        return self.id, inserted_step_id

    @status_handler(on_error="NO_UPDATE")
    def update_status(self):
        """
        Updates cluster's status based on EMR status.

        Raises:
            UpdateStatusError: when the status can't be retrieved from EMR.

        """

        try:
            response = self.emr_handler.describe_cluster(cluster_id=self.id)
            # Dump dict to json with default=str to overcome 'datetime object not serializable'.
            # Then load it again. Masterful.
            self.properties_snapshot = json.loads(json.dumps(response, default=str))
        except UpdateStatusError as e:
            logger.error(
                "Error updating status for cluster {} - msg: {}".format(self.id, e)
            )
            raise

        self.status = response["Cluster"]["Status"]["State"]

        if not self.terminate_on and self.status == Cluster.UNASSIGNED_STATUS:

            self.terminate_on = datetime.utcnow() + timedelta(minutes=15)

    @status_handler(on_error="TERMINATED_WITH_ERRORS", on_success="TERMINATED")
    def terminate(self):
        """
        Terminates a cluster.

        Raises:
            UnableToTerminateClusterError: if cluster can not be terminated.

        """
        try:
            logger.info("Terminating cluster {}".format(self.id))
            self.emr_handler.terminate_cluster(cluster_id=self.id)
        except UnableToTerminateClusterError as e:
            logger.error("Terminating cluster {} - msg: {}".format(self.id, e))
            raise

    def is_terminated(self) -> bool:
        return self.status in self.TERMINATED_STATUS

    def metrics(self) -> (str, dict):
        return datadog_metric(
            self.__class__.__name__.lower(),
            {"id": self.id, "user": self.user, "cluster_config_name": self.job_flow_config.get("Name")},
        )


class StepsCluster(Cluster):

    __tablename__ = "step_clusters"
    __mapper_args__ = {"concrete": True}

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    id = db.Column(db.String(64), primary_key=True)
    status = db.Column(db.String(64), nullable=False)
    terminate_on = db.Column(db.DateTime)
    assigned_steps = db.Column(db.PickleType, nullable=False)
    credentials = db.Column(Encrypted, nullable=False)
    job_flow_config = db.Column(db.JSON, nullable=False)
    properties_snapshot = db.Column(db.JSON, default={}, nullable=False)
    # waiting_since = db.Column(db.DateTime)
    # last_added_step = db.Column(db.DateTime, default=datetime.utcnow())

    # All step_clusters are created by the API manager.
    # We do not save this in the DB.
    user = "manager"


class ClusterConfiguration(Base):
    """A class for cluster configurations to be stored and retrieved from s3.
    These are used in requests to instantiate clusters with a particular configuration."""

    __tablename__ = "configurations"

    id = db.Column(db.Integer, primary_key=True)
    s3_uri = db.Column(db.String(128), nullable=False)
    name = db.Column(db.String(128), nullable=False)
    version = db.Column(db.String(64), nullable=False)
    user = db.Column(db.String(64), nullable=False)

    CONFIGURATION_S3_URI = "s3://bi.config.dl/cluster-manager/{}/{}.json"

    def __init__(self, name: str, job_flow_config: dict):
        self.name = name
        self.job_flow_config = job_flow_config
        self.version = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        self.s3_uri = self.configuration_uri()
        self.s3_handler = S3Handler(
            aws_access_key_id=os.getenv("SECRET_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_SECRET_KEY"),
        )

        self.user = current_user.get_id()

    @orm.reconstructor
    def init_on_load(self):
        # CAREFUL. Attributes that are not persisted
        # in the DB can only be accessed after the
        # reconstructor method has instantiated them.
        self.s3_handler = S3Handler(
            aws_access_key_id=os.getenv("SECRET_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_SECRET_KEY"),
        )

    def configuration_uri(self):
        """Generates the s3_uri for a given configuration name."""
        return self.CONFIGURATION_S3_URI.format(self.name, self.version)

    def upload(self):
        """Stores the configuration in s3. The path is determined by the
        name of the config and its version.

        Raises:
            UnableToUploadContentError: when the s3_handler fails.

        """

        configuration_content = json.dumps(self.job_flow_config)
        try:
            self.s3_handler.upload(
                destination_uri=self.s3_uri, content=configuration_content
            )
        except UnableToUploadContentError as e:
            logger.error(
                "Unable to upload config {} - msg: {}".format(self.job_flow_config, e)
            )
            raise

    def download(self):
        """Retrieves the config from s3 and stores it as a JSON in the
        'job_flow_config' attribute of the object.
        """
        configuration_content = self.s3_handler.download(self.s3_uri)
        configuration = json.loads(configuration_content)
        self.job_flow_config = configuration
