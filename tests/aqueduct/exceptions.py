class AqueductError(Exception):
    """Base class for all Aqueduct errors."""


class FlowError(AqueductError):
    """Flow can raise this if something was wrong."""


class NotRunningError(FlowError):
    """Flow can raise this if it's not already running."""


class BadReferenceCount(AqueductError, ValueError):
    pass
