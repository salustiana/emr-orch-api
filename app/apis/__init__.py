from .ping import api as ping
from .v1.clusters import api as cluster
from .v1.step_clusters import api as steps_cluster
from .v1.manager import api as manager
from .v1.steps import api as step
from app.apis.login import api as login

from .v1.cluster_config import api as cluster_config

from flask import Blueprint
from flask_restx import Api


__VERSION__ = 1


def versioneer(version: int) -> (str, str):
    """
    Given an int version (1, 2, 3) representing the API version, returns the
    formatted version to use in `Api` definition as well as to be used in `Blueprint`'s
    `url_prefix`

    """
    return "{:.1f}".format(version), "v{}".format(version)


api_version, url_prefix_version = versioneer(__VERSION__)


# Health Check API & Blueprint
health_check_bp = Blueprint("health-check", __name__)
health_check_api = Api(
    health_check_bp,
    title="Spark Clusters Manager Health Check",
    version=api_version,
    description="Health Check Blueprint",
)
health_check_api.add_namespace(ping, path="/ping")


# Spark Cluster Manager Login
login_blueprint = Blueprint(
    "login", __name__, url_prefix="/bi/analytics-cluster-manager"
)
login_api = Api(
    login_blueprint,
    title="Spark Clusters Manager Login",
    version=api_version,
    description="Spark Cluster Login",
)
login_api.add_namespace(login, path="/login")


# Spark Cluster Manager API & Blueprint
blueprint = Blueprint(
    "api",
    __name__,
    url_prefix="/bi/analytics-cluster-manager/{}".format(url_prefix_version),
)
api = Api(
    blueprint,
    title="Spark Cluster Manager",
    version=api_version,
    description="Spark Cluster CRUD",
)

api.add_namespace(cluster, path="/cluster")
api.add_namespace(steps_cluster, path="/steps_cluster")
api.add_namespace(manager, path="/manage")
api.add_namespace(step, path="/step")
api.add_namespace(cluster_config, path="/config")


blueprints = [health_check_bp, login_blueprint, blueprint]
