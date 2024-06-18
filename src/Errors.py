class OrefAPIException(Exception):
    def __init__(self, message):
        super().__init__(message)


class WrongSettlementException(Exception):
    def __init__(self, message):
        super().__init__(message)


class NoAlarmsException(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidSettlement(Exception):
    def __init__(self, message):
        super().__init__(message)


class RateLimitException(Exception):
    def __init__(self, message):
        super().__init__(message)


class SqlDatabaseException(Exception):
    def __init__(self, message):
        super().__init__(message)
