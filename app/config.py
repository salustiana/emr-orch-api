from app.utils import is_prod

from melitk.melipass import get_secret


class Config(object):

    # Flask Config
    DEBUG = False
    TESTING = False
    SECRET_KEY = get_secret("FLASK_SECRET_KEY")

    # ORM Config
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # DB Config
    DIALECT = "mysql"
    DRIVER = "pymysql"
    PORT = 6612
    DATABASE = "testclust"
    HOST = "analyticsclus00.master.mlaws.com"

    LDAP_PORT = 389
    LDAP_HOST = "us-admeli.name"
    LDAP_USE_SSL = False
    LDAP_READONLY = False
    LDAP_CHECK_NAMES = True
    LDAP_BASE_DN = "OU=MercadoLibre,DC=ml,DC=com"
    LDAP_USER_SEARCH_SCOPE = "SUBTREE"
    LDAP_USER_LOGIN_ATTR = "sAMAccountName"
    LDAP_USER_OBJECT_FILTER = "(objectClass=user)"
    LDAP_GET_USER_ATTRIBUTES = "distinguishedName"
    LDAP_BIND_USER_DN = get_secret("LDAP_USER_BIND")
    LDAP_BIND_USER_PASSWORD = get_secret("LDAP_PASS_BIND")

    @property
    def SQLALCHEMY_DATABASE_URI(self):
        return "{}+{}://{}:{}@{}:{}/{}".format(
            self.DIALECT,
            self.DRIVER,
            self.USER,
            self.PASSWORD,
            self.HOST,
            self.PORT,
            self.DATABASE,
        )


class ProductionConfig(Config):

    # Based on: https://meli.workplace.com/notes/fury-users/mysql-proxy-cambios-en-la-conexi%C3%B3n-entre-apps-de-fury-y-bases-mysql/304503510950962/
    USER = get_secret("DB_USER")
    PASSWORD = get_secret("DB_PASSWORD")


class DevelopmentConfig(Config):

    # Flask Config
    DEBUG = True
    SECRET_KEY = "mysecretkey"

    # DB Config
    # dialect+driver://username:password@host:port/database
    # For development, we use a local hosted database, launched with the `docker-compose.yml`
    USER = "user"
    PASSWORD = "password"
    HOST = "localhost"
    DATABASE = "db"
    PORT = 3306


def environment_config() -> Config:
    if is_prod():
        return ProductionConfig()
    return DevelopmentConfig()
