from http import HTTPStatus
from melitk import logging

from app.login.auth import validate_user
from app.login.errors import UnableToLoginError
from app.apis.schemas import AuthSchema

from flask import request
from flask_login import current_user, login_user
from flask_restx import Namespace, Resource
from marshmallow import ValidationError

api = Namespace("Login", description="Login endpoint")

# Specific logger for `login`
logger = logging.getLogger(__name__)


@api.route("/")
class Login(Resource):
    @api.doc("Login endpoint")
    def post(self):
        try:
            username, password = self.parse_auth_body(request.get_json(force=True))
        except ValidationError as e:
            api.abort(HTTPStatus.BAD_REQUEST, message=str(e))

        if current_user.is_authenticated:
            return "is_authenticated", 200

        try:
            user = validate_user(username, password)
        except UnableToLoginError:
            return "unauthorized", 401
        else:
            login_user(user)

        return "OK", 200

    def parse_auth_body(self, body: dict):
        try:
            # auth_schema podría ser variable de clase
            auth_schema = AuthSchema().load(body)
        except ValidationError as e:
            logger.error("Unable to parse body - msg: {}".format(e))
            raise
        return auth_schema["username"], auth_schema["password"]
