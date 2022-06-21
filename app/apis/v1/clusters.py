from http import HTTPStatus
from datetime import datetime, timedelta

from app.extensions import db
from app.utils import logger
from app.core.errors import (
    CreateClusterError,
    StepCreationError,
    UnableToAssignStepError,
)
from app.core.models import Cluster, Step
from app.core.aws_handler import EMRHandler
from app.apis.v1.utils import generate_config
from app.apis.v1.steps import step_model

from flask import request
from flask_login import login_required, current_user
from flask_restx import Namespace, Resource, fields
from melitk import metrics
from melitk.metrics.exceptions import MetricsError
from sqlalchemy.exc import SQLAlchemyError


api = Namespace("Clusters", description="CRUD for free clusters")


def minutes_remaining(cluster):
    if cluster.status in cluster.TERMINATED_STATUS:
        return None
    return (cluster.terminate_on - datetime.utcnow()).total_seconds() / 60


cluster_model = api.model(
    "Cluster",
    {
        "id": fields.String(required=True, description="AWS ID for Cluster"),
        "status": fields.String(required=True, description="Cluster status"),
        # TODO: this is +- 5 minutes
        "minutes_remaining": fields.Integer(
            attribute=minutes_remaining,
            description="Remaining time in minutes before the cluster is terminated",
        ),
        "logs_uri": fields.String(description="URI where logs are stored"),
        "ip_address": fields.String(description="Cluster's IP provided by AWS"),
        "tags": fields.Raw(description="Tags associated with the cluster"),
        "created_on": fields.String(description="Creation date on AWS"),
        "ready_on": fields.String(description="Ready since"),
        "ended_on": fields.String(description="End date on AWS"),
        "assigned_steps": fields.List(
            fields.String, description="Steps assigned to cluster"
        ),
    },
)


@api.route("/")
class Clusters(Resource):
    @api.marshal_list_with(cluster_model, code=HTTPStatus.OK)
    def get(self):
        """Queries the API DB for all clusters and returns information about them."""

        clusters = Cluster.query.filter_by(**request.args).all()

        return clusters

    @login_required
    @api.marshal_with(cluster_model, code=HTTPStatus.CREATED)
    def post(self):

        """Submits a cluster for its instantiation in AWS and it is inserted into the API database.
        a JSON must be passed as a header to specify the configuration of the cluster."""

        # TODO: validate input
        cluster_definition = request.get_json(force=True)

        job_flow_config = generate_config(cluster_definition["cluster_config"])
        del cluster_definition["cluster_config"]
        cluster_definition["job_flow_config"] = job_flow_config

        try:
            cluster = Cluster(**cluster_definition)
            cluster.create()
            db.session.add(cluster)
            db.session.commit()

        except KeyError as e:
            db.session.rollback()
            api.abort(
                HTTPStatus.BAD_REQUEST,
                message="The received body is different than expected - msg: {}".format(
                    e
                ),
            )
        except CreateClusterError as e:
            db.session.rollback()
            api.abort(HTTPStatus.BAD_REQUEST, message=str(e))
        except SQLAlchemyError as e:
            db.session.rollback()
            api.abort(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error inserting the cluster into the db - msg: {}.".format(e),
            )
        else:
            datadog_metric = cluster.metrics()

            try:
                metrics.record_count(
                    name=datadog_metric.metric_name,
                    increment=1,
                    tags=datadog_metric.tags,
                )
            except MetricsError as e:
                logger.info(
                    "Unable to post {} metric with tags {} - msg: {}".format(
                        datadog_metric.metric_name, datadog_metric.tags, e
                    )
                )

        return cluster


@api.route("/<string:cluster_id>")
class ManageCluster(Resource):
    @api.marshal_with(cluster_model, code=HTTPStatus.OK)
    def get(self, cluster_id: str):
        """Returns information about an ID-specified cluster."""

        cluster = Cluster.query.get(cluster_id)
        if cluster:
            return cluster
        else:
            api.abort(
                HTTPStatus.NOT_FOUND, f"Cluster {cluster_id} does not exist in the DB"
            )

    @login_required
    @api.marshal_list_with(cluster_model, code=HTTPStatus.OK)
    def delete(self, cluster_id: str):
        """Terminates a cluster in AWS.
        A JSON with the cluster's ID and credentials must be provided as a header."""

        cluster_to_terminate = Cluster.query.get(cluster_id)

        if not cluster_to_terminate:
            api.abort(
                HTTPStatus.NOT_FOUND,
                f"Cluster {cluster_id} does not exist in the DB",
            )
        if cluster_to_terminate.user != current_user.get_id():
            api.abort(
                HTTPStatus.UNAUTHORIZED,
                f"You are logged in as {current_user.get_id()}. Only {cluster_to_terminate.user} can terminate this cluster.",
            )

        cluster_to_terminate.terminate()

        db.session.add(cluster_to_terminate)
        db.session.commit()

        return cluster_to_terminate


@api.route("/<string:cluster_id>/extend")
class ExtendCluster(Resource):
    @login_required
    @api.marshal_with(cluster_model, code=HTTPStatus.OK)
    def put(self, cluster_id: str):
        """Extends a cluster's lifetime for a given amount of minutes."""

        body = request.get_json(force=True)
        try:
            extension_minutes = body["minutes"]
        except KeyError:
            api.abort(
                HTTPStatus.BAD_REQUEST,
                message=f"The received body is different than expected - msg: {e}",
            )

        cluster = Cluster.query.get(cluster_id)
        if not cluster:
            api.abort(
                HTTPStatus.NOT_FOUND, f"Cluster {cluster_id} does not exist in the DB"
            )

        cluster.terminate_on += timedelta(minutes=extension_minutes)
        db.session.commit()
        return cluster


@api.route("/<string:cluster_id>/add_step")
class StepInsertion(Resource):
    @login_required
    @api.marshal_with(step_model, code=HTTPStatus.OK)
    def post(self, cluster_id: str):
        """Submits a step to be inserted into the specified cluster."""

        cluster = Cluster.query.get(cluster_id)
        if not cluster:
            api.abort(
                HTTPStatus.NOT_FOUND, f"Cluster {cluster_id} does not exist in the DB"
            )

        # TODO: validate input
        step_definition = request.get_json(force=True)
        step_definition["job_flow_config"] = cluster.job_flow_config
        del step_definition["cluster_config"]

        try:
            step = Step(**step_definition)
            cluster_id, step_id = cluster.add_step(step)
            step.check_in(cluster_id, step_id)
            db.session.add(step)
            db.session.commit()

        except KeyError as e:
            db.session.rollback()
            api.abort(
                HTTPStatus.BAD_REQUEST,
                message="The received body is different than expected - msg: {}".format(
                    e
                ),
            )

        except StepCreationError as e:
            db.session.rollback()
            api.abort(HTTPStatus.BAD_REQUEST, message=str(e))

        except UnableToAssignStepError as e:
            api.abort(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error inserting the step into the cluster - msg: {}.".format(
                    e
                ),
            )

        except SQLAlchemyError as e:
            db.session.rollback()
            api.abort(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Error inserting the step into the db - msg: {}.".format(e),
            )
        else:
            datadog_metric = step.metrics()

            try:
                metrics.record_count(
                    name=datadog_metric.metric_name,
                    increment=1,
                    tags=datadog_metric.tags,
                )
            except MetricsError as e:
                logger.info(
                    "Unable to post {} metric with tags {} - msg: {}".format(
                        datadog_metric.metric_name, datadog_metric.tags, e
                    )
                )

        return step
