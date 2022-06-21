class ClusterManagerBaseError(Exception):
    pass


# Steps exceptions #


class StepBaseError(ClusterManagerBaseError):
    pass


class UnableToUploadScriptError(StepBaseError):
    pass


class StepCreationError(StepBaseError):
    pass


class NoScriptError(StepCreationError):
    pass


class KeyExistsError(StepCreationError):
    pass


class StepCancelError(StepBaseError):
    pass


# AWS Handler exceptions #


class AWSHandlerError(Exception):
    pass


class CredentialsError(AWSHandlerError):
    pass


class UnableToUploadContentError(AWSHandlerError):
    pass


# Cluster custom exceptions #


class ClusterBaseError(ClusterManagerBaseError):
    pass


class CreateClusterError(ClusterBaseError):
    pass


class UpdateStatusError(ClusterBaseError):
    pass


class UnableToAssignStepError(ClusterBaseError):
    pass


class UnableToTerminateClusterError(ClusterBaseError):
    pass


# Manager endpoint exceptions #


class ManagerBaseError(Exception):
    pass


class ParseBodyError(ManagerBaseError):
    pass
