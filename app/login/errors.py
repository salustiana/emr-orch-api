class AuthenticationBaseError(Exception):
    pass


class UnableToLoginError(AuthenticationBaseError):
    pass
