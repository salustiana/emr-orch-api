from app.extensions import db, login_manager, ldap_manager

from flask import Flask


def create_app():
    app = Flask(__name__)

    # accepts both /endpoint and /endpoint/ as valid URLs
    app.url_map.strict_slashes = False

    config_app(app)

    register_blueprints(app)
    register_extensions(app)
    custom_config(app)

    return app


def config_app(app):
    """Set Flask configuration parameters."""
    from app.config import environment_config

    app.config.from_object(environment_config())


def register_blueprints(app):
    """Register Flask namespaces."""
    from app.apis import blueprints

    [app.register_blueprint(blueprint) for blueprint in blueprints]


def register_extensions(app):
    """Register Flask extensions."""

    db.init_app(app)

    login_manager.init_app(app)
    ldap_manager.init_app(app)

    with app.app_context():
        # Importing models for creation on application start
        # Could be removed when DB is already app & running
        from app.login.auth import User
        from app.core.models import Cluster, Step

        # db.drop_all()
        db.create_all()


def custom_config(app):
    """Configs that don't follow flask documentation but are required by our domain."""

    # Related to https://flask-restful.readthedocs.io/en/0.3.6/quickstart.html?highlight=ERROR_404_HELP
    app.config["ERROR_404_HELP"] = False
