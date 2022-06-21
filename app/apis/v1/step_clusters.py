from http import HTTPStatus

from app.extensions import db
from app.core.models import StepsCluster

from flask import request
from flask_login import login_required
from flask_restx import Namespace, Resource, fields


api = Namespace("Step Clusters", description="CRUD for step assigned clusters")

steps_cluster_model = api.model(
    "Steps Cluster",
    {
        "id": fields.String(required=True, description="AWS ID for Cluster"),
        "status": fields.String(required=True, description="Cluster status"),
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
    @api.marshal_list_with(steps_cluster_model, code=HTTPStatus.OK)
    def get(self):
        """Queries the API DB for all clusters and returns information about them."""

        clusters = StepsCluster.query.filter_by(**request.args).all()

        return clusters


@api.route("/<string:cluster_id>")
class ManageCluster(Resource):
    @api.marshal_with(steps_cluster_model, code=HTTPStatus.OK)
    def get(self, cluster_id: str):
        """Returns information about an ID-specified cluster."""

        cluster = StepsCluster.query.get(cluster_id)
        if not cluster:
            api.abort(
                HTTPStatus.NOT_FOUND, f"Cluster {cluster_id} does not exist in the DB"
            )

        return cluster
