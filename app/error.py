class ThreadStopException(Exception):
    def __init__(self, message):
        super().__init__(message)
        
class TaskCancelledException(Exception):
    pass


class QueryValidationError(ValueError):
    pass


class LogicValidationError(QueryValidationError):
    pass
