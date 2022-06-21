from app.utils import logger
from app.extensions import db, login_manager, ldap_manager
from app.login.errors import UnableToLoginError

from flask_login import UserMixin
from tiger_python_helper.services.tiger_service import TigerService
from tiger_python_helper.exceptions.tiger_authentication_error import TigerAuthenticationException



class User(UserMixin, db.Model):
    # User's AD will be its ID
    id = db.Column(db.String(64), index=True, unique=True, primary_key=True)


def is_valid_user(user: str) -> bool:
    response = ldap_manager.get_user_info_for_username(user)
    if response:
        return True
    return False


def validate_user(username: str, password: str):
    try:
        service = TigerService()
        service.get_user_token(username, password)
    except TigerAuthenticationException as e:
        logger.info("Unable to login user {} - {}".format(username, e))
        raise UnableToLoginError("Unable to log in")
    return get_or_save_user(username)


def get_or_save_user(username):
    user = User.query.get(username)
    if not user:
        user = User(id=username)
        db.session.add(user)
        db.session.commit()
    return user


@login_manager.user_loader
def load_user(id):
    return User.query.get(id)
