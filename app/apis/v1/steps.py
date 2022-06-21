from http import HTTPStatus

from app.extensions import db
from app.notifications import steps_producer
from app.utils import logger
from app.core.models import Step
from app.core.errors import StepCreationError
from app.apis.v1.utils import generate_config

from flask import request
from flask_login import login_required, current_user
from flask_restx import Namespace, Resource, fields
from melitk import metrics
from melitk.bigqueue.exceptions import BigQueueInternalError, InvalidMessageError
from melitk.metrics.exceptions import MetricsError
from sqlalchemy.exc import SQLAlchemyError


api = Namespace("Steps", description="Spark Steps CRUD")

step_model = api.model(
    "Step",
    {
        "id": fields.Integer(required=True, description="Step ID"),
        "step_id": fields.String(description="Step ID in AWS"),
        "name": fields.String(required=True, description="Step name"),
        "status": fields.String(required=True, description="Step status"),
        "cluster_id": fields.String(
            description="Cluster to which this step was assigned to"
        ),
        "logs_uri": fields.String(description="Logs path in s3"),
        "custom_metadata": fields.Raw(
            description="Custom metadata associated to the step, for tracking costs, etc."
        ),
        "created_on": fields.String(description="Creation date on AWS"),
        "started_on": fields.String(description="Start date on AWS"),
        "ended_on": fields.String(description="End date on AWS"),
    },
)


@api.route("/")
class Steps(Resource):
    @login_required
    @api.marshal_with(step_model, code=HTTPStatus.CREATED)
    def post(self):

        """Submits a step to the API database so that it can be inserted into a cluster for its execution.
        a JSON must be passed as a header to specify the configuration of the step."""

        # TODO: validate input
        # Validation should include checking for cluster_config if
        # job_flow_config is not provided
        step_definition = request.get_json(force=True)

        job_flow_config = generate_config(step_definition["cluster_config"])
        del step_definition["cluster_config"]
        step_definition["job_flow_config"] = job_flow_config

        try:
            print(step_definition)
            step = Step(**step_definition)
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

        # TODO: publish is disabled because it results in multiple steps added to the same WAITING cluster
        # try:
        #     steps_producer.publish(message={"steps": [step.id]})
        # except (InvalidMessageError, BigQueueInternalError) as e:
        #     api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, e)

        return step

    @api.marshal_list_with(step_model, code=HTTPStatus.OK)
    def get(self):
        """Queries the API DB for all steps and returns information about them."""

        steps = Step.query.filter_by(**request.args).all()

        return steps


@api.route("/<int:step_id>")
class ManageStep(Resource):
    @api.marshal_with(step_model, code=HTTPStatus.OK)
    def get(self, step_id: int):
        """Returns information about an ID-specified step."""

        step = Step.query.get(step_id)
        if step:
            return step
        else:
            api.abort(HTTPStatus.NOT_FOUND, f"Step {step_id} does not exist in the DB")

    @login_required
    @api.marshal_with(step_model, code=HTTPStatus.OK)
    def put(self, step_id: int):
        """Updates fields from a step.
        A JSON with fields to be modified and its values must be provided as a header."""

        body = request.get_json(force=True)
        step_to_update = Step.query.get(step_id)
        for key in body:
            setattr(step_to_update, key, body[key])

        db.session.add(step_to_update)
        db.session.commit()

        return step_to_update

    @login_required
    @api.marshal_list_with(step_model, code=HTTPStatus.OK)
    def delete(self, step_id: int):
        """Cancels the step's execution.
        A JSON with credentials must be provided as a header."""

        step_to_cancel = Step.query.get(step_id)

        if not step_to_cancel:
            api.abort(HTTPStatus.NOT_FOUND, f"Step {step_id} does not exist in the DB")
        if step_to_cancel.user != current_user.get_id():
            api.abort(
                HTTPStatus.UNAUTHORIZED,
                f"You are logged in as {current_user.get_id()}. Only {cluster_to_terminate.user} can cancel this step.",
            )

        step_to_cancel.cancel()

        db.session.add(step_to_cancel)
        db.session.commit()

        return step_to_cancel
