from flask_ldap3_login import LDAP3LoginManager
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy

ldap_manager = LDAP3LoginManager()
login_manager = LoginManager()
db = SQLAlchemy()
