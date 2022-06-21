from app.core.models import ClusterConfiguration

from flask_restx import abort
from sqlalchemy.orm.exc import NoResultFound
from http import HTTPStatus


def generate_config(cluster_config: dict) -> dict:
    """
    Parses the provided 'cluster_config' field in the
    request body and generates a job_flow_config accordingly
    by fetching base config from the database and modifying
    the requested custom parameters.
    """
    # Parse the 'cluster_config' dict from the request body.
    job_flow_config = cluster_config.get("job_flow_config")
    if job_flow_config:
        if cluster_config.get("name"):
            abort(
                HTTPStatus.BAD_REQUEST,
                message="The received body is different than expected - msg: "
                "You must provide either a 'job_flow_config' or a 'name'. Not both.",
            )
        return job_flow_config

    name = cluster_config["name"]
    version = cluster_config.get("version")
    customizations = cluster_config.get("custom_parameters")
    bootstrap_actions = cluster_config.get("bootstrap_actions")

    # Query the DB for the requested base config.
    configs_query = ClusterConfiguration.query.filter_by(name=name).order_by(
        ClusterConfiguration.version.desc()
    )

    if version:
        try:
            config = configs_query.filter_by(version=version).one()
        except NoResultFound:
            abort(
                HTTPStatus.NOT_FOUND,
                f"Configuration {name} with version: {version} does not exist in the DB",
            )
    else:
        # Return the first row by sorting version in descending order.
        # Since version is year-month-day-hour-minute-second-fraction,
        # This works fine.
        config = configs_query.first()
        if not config:
            abort(
                HTTPStatus.NOT_FOUND, f"Configuration {name} does not exist in the DB"
            )

    config.download()
    # This just makes the name shorter. Remember these two are the same dict!
    job_flow_config = config.job_flow_config

    if bootstrap_actions:
        # TODO: validate this with a list of schemas for BootstrapActions
        try:
            job_flow_config["BootstrapActions"] += bootstrap_actions
        except KeyError:
            job_flow_config["BootstrapActions"] = bootstrap_actions

    # Applies the requested customizations to the base config.
    if customizations:
        CUSTOMIZABLE_PARAMETERS = {"instance_type", "instance_count", "volume_size"}
        for key, value in customizations.items():
            if key not in CUSTOMIZABLE_PARAMETERS:
                abort(
                    HTTPStatus.BAD_REQUEST,
                    message=f"{key} is not a customizable parameter. "
                    f"Accepted parameters are: {', '.join(CUSTOMIZABLE_PARAMETERS)}",
                )

            if value and key == "instance_type":
                job_flow_config["Instances"]["InstanceGroups"][1][
                    "InstanceType"
                ] = value

            if value and key == "instance_count":
                job_flow_config["Instances"]["InstanceGroups"][1][
                    "InstanceCount"
                ] = value

            if value and key == "volume_size":
                job_flow_config["Instances"]["InstanceGroups"][0]["EbsConfiguration"][
                    "EbsBlockDeviceConfigs"
                ][0]["VolumeSpecification"]["SizeInGB"] = value
                job_flow_config["Instances"]["InstanceGroups"][1]["EbsConfiguration"][
                    "EbsBlockDeviceConfigs"
                ][0]["VolumeSpecification"]["SizeInGB"] = value

    return job_flow_config
