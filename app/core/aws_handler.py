from urllib.parse import urlparse

from app.utils import logger
from app.core.errors import (
    CredentialsError,
    CreateClusterError,
    UnableToAssignStepError,
    UpdateStatusError,
    StepCancelError,
    UnableToTerminateClusterError,
    UnableToUploadContentError,
)

import boto3
from botocore.exceptions import ClientError, ParamValidationError


DEFAULT_AWS_REGION = "us-east-1"

DEFAULT_ENCODING = "utf-8"


def service_client(
    service: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_session_token: str = None,
    region_name: str = DEFAULT_AWS_REGION,
):
    """Creates the client for the required service with the given credentials."""

    return boto3.client(
        service,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name,
    )


class AWSHandler:
    """Base class for AWS services handler. To be subclassed by either
    S3Handler or EMRHandler."""

    def __init__(
        self,
        service_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str = None,
        region_name: str = DEFAULT_AWS_REGION,
    ):
        self.client = service_client(
            service_name,
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
            region_name,
        )


class S3Handler(AWSHandler):
    """ Defines methods to interact with AWS' s3 service. """

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str = None,
        region_name: str = DEFAULT_AWS_REGION,
    ):
        super().__init__(
            "s3",
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
            region_name,
        )

    def split_s3_uri(self, s3_uri: str) -> (str, str):
        """Helper function to get bucket and key names from an s3 URI.
        Args:
            s3_uri (str)

        Returns:
            tuple (str, str): (bucket name, key name)
        """

        parsed_s3_uri = urlparse(s3_uri)
        bucket, key = parsed_s3_uri.netloc, parsed_s3_uri.path.lstrip("/")
        return bucket, key

    def file_exists(self, destination_uri: str) -> bool:
        """
        Returns True if Key exists in s3; otherwise False.

        Returns:
            bool: whether the requested (bucket, key) combination exist or not.

        Raises:
            CredentialsError: if the credentials used to look for the (bucket, key)
                are invalid.

        """
        bucket, key = self.split_s3_uri(destination_uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            if e.response["Error"]["Code"] == "403":
                raise CredentialsError(
                    "The credentials for uploading the script to s3 are invalid"
                )
            raise

    def upload(self, destination_uri: str, content: str):
        """Uploads objects to s3.

        Args:
            destination_uri (str): the s3 path where the content must be stored.
            content (str): the content to be stored.

        Returns:
            str: the stored content.

        Raises:
            UnableToUploadContentError: when the put_object method from boto3 fails.
        """

        bucket, key = self.split_s3_uri(destination_uri)
        try:
            self.client.put_object(Body=content, Bucket=bucket, Key=key)
        except ClientError as e:
            raise UnableToUploadContentError(
                "Unable to upload content - msg: {}".format(e)
            )

    def download(self, source_uri: str, encoding: str = DEFAULT_ENCODING):
        """Method to download objects from s3.
        Args:
            source_uri (str): the path in s3 to download from.
            encoding (str): the encoding type.

        Returns:
            str: the decoded content from the s3 download.
        """

        bucket, key = self.split_s3_uri(source_uri)
        content = (
            self.client.get_object(Bucket=bucket, Key=key)["Body"]
            .read()
            .decode(encoding)
        )
        return content


class EMRHandler(AWSHandler):
    """ Defines methods to interact with AWS' EMR service. """

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str = None,
        region_name: str = DEFAULT_AWS_REGION,
    ):
        super().__init__(
            "emr",
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
            region_name,
        )

    def check_permissions(self):
        """Try to list clusters to ckeck permissions."""
        try:
            self.client.list_clusters()
        except ClientError:
            logger.info(
                "The emr credentials provided are either invalid or do not have the necessary permissions"
            )
            raise CredentialsError(
                "The emr credentials provided are either invalid or do not have the necessary permissions"
            )

    def create_cluster(self, job_flow_config: dict):
        """ Method to instantiate clusters in AWS' EMR."""
        try:
            response = self.client.run_job_flow(**job_flow_config)
        except (ClientError, ParamValidationError) as e:
            raise CreateClusterError("Error creating cluster - msg: {}".format(e))
        cluster_id = response["JobFlowId"]
        return cluster_id

    def add_step_to_cluster(self, cluster_id: str, step):
        """Method to insert steps into active EMR clusters.

        Args:
            cluster_id (str): the AWS ID representing the cluster where the step must be inserted.
            step (Step()): the step object to be inserted in the cluster.

        Returns:
            str: the inserted step's AWS ID.

        Raises:
            UnableToAssignStepError: when the EMR client fails.
        """
        try:
            response = self.client.add_job_flow_steps(
                JobFlowId=cluster_id, Steps=[step.step_config]
            )
        except (ClientError, ParamValidationError) as e:
            raise UnableToAssignStepError(
                "Unable to add step {} to cluster {} - msg: {}".format(
                    step.id, cluster_id, e
                )
            )

        inserted_step_id = response["StepIds"][0]
        return inserted_step_id

    def describe_cluster(self, cluster_id: str):
        """Get cluster's status and timeline from AWS.

        Args:
            cluster_id (str): the AWS ID representing the cluster which must be described.

        Returns:
            response (dict): the response from AWS.

        Raises:
            UpdateStatusError: when the client fails to describe the cluster.
        """
        try:
            response = self.client.describe_cluster(ClusterId=cluster_id)
        except ClientError as e:
            raise UpdateStatusError(
                "Error updating status for cluster {} - msg: {}".format(cluster_id, e)
            )

        return response

    def terminate_cluster(self, cluster_id: str):
        """Terminates clusters in AWS.

        Args:
            cluster_id (str): the AWS ID representing the cluster to be terminated.

        Raises:
            UnableToTerminateClusterError: when the terminate_job_flows method from boto3 fails.
        """
        try:
            self.client.terminate_job_flows(JobFlowIds=[cluster_id])
        except ClientError as e:
            raise UnableToTerminateClusterError(
                "Error terminating cluster {} - msg: {}".format(cluster_id, e)
            )

    def cancel_steps(self, cluster_id: str, step_id: str):
        """Cancels steps from running AWS clusters.

        Args:
            cluster_id (str): the AWS ID representing the cluster containing the step to be cancelled.
            step_id (str): the AWS ID representing the step to be cancelled.

        Raises:
            StepCancelError: when the cancel_steps method from boto3 fails.
        """
        try:
            response = self.client.cancel_steps(ClusterId=cluster_id, StepIds=[step_id])
        except Exception as e:
            raise StepCancelError(
                "Error cancelling step {} in cluster {} - msg: {}".format(
                    step_id, cluster_id, e
                )
            )

        return response

    def describe_step(self, cluster_id: str, step_id: str):

        """Describe a step assigned to a given cluster.

        Args:
            cluster_id (str): the AWS ID representing the cluster containing the step to be described.
            step_id (str): the AWS ID representing the step to be described.

        Returns:
            response (dict).

        Raises:
            UpdateStatusError: when the client fails to describe the step.
        """
        try:
            response = self.client.describe_step(ClusterId=cluster_id, StepId=step_id)
        except ClientError as e:
            raise UpdateStatusError(
                "Error updating status for step {} - msg: {}".format(step_id, e)
            )

        return response
