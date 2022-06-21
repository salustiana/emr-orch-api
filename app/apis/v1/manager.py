from http import HTTPStatus
from typing import List, Union, Callable

from app.apis.schemas import BigQueueBodySchema, FuryJobBodySchema
from app.core import Manager
from app.core.errors import ParseBodyError

from flask import request
from flask_restx import Namespace, Resource
from marshmallow import ValidationError


api = Namespace("Manager", description="Spark steps manager endpoint")


@api.route("/")
class ClusterManager(Resource):
    def post(self):
        body = request.get_json()

        steps = get_steps_from_body(body)

        with Manager(step_ids=steps) as manager:
            manager.run()

        return HTTPStatus.NO_CONTENT


def get_steps_from_body(body) -> Union[List[int], None]:
    """
    Returns:
        Union[List[int], None]: list of step id's if received or None
    Raises:
        ValidationError: when the body can't be deserialized.
    """

    if not body:
        return None

    try:
        parser = _get_body_parser(body)
    except ParseBodyError as e:
        api.abort(HTTPStatus.BAD_REQUEST, message=f"Error in request to /manage: {e}")

    steps = parser(body)
    return steps


def _get_body_parser(body: dict) -> Callable:
    """
    Returns the corresponding parser depending
    on the body type.
    Supported types:
        - Fury Job body.
        - BigQueue body.

    Raises:
        ParseBodyError: when no body type could be specified.
    """
    if body.get("execution_id"):
        return _get_none_from_fury_job_body

    if body.get("topic"):
        return _get_steps_from_bigq_body

    raise ParseBodyError(
        "Unable to identify request body type.  Valid body types are: "
        "None (no body); "
        "Fury Job body (must contain 'execution_id' field); "
        "BigQueue body (must contain 'topic' field)"
    )


def _get_none_from_fury_job_body(body: dict) -> None:
    """
    Given a Fury Job body, returns None if
    validation succeeds.
    """
    try:
        FuryJobBodySchema().validate(body)
        return None

    except ValidationError as e:
        api.abort(
            HTTPStatus.BAD_REQUEST, message=f"Error parsing BigQ request: {e.messages}"
        )


def _get_steps_from_bigq_body(body: dict) -> List[int]:
    """ "
    Given a BigQ body, returns the list of steps received.

    Returns:
        List[int]: list of step id's
    """
    try:
        bigq_message = BigQueueBodySchema().load(body)
        return bigq_message["msg"]["steps"]

    except ValidationError as e:
        api.abort(
            HTTPStatus.BAD_REQUEST, message=f"Error parsing BigQ request: {e.messages}"
        )
