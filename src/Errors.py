class RetrieveDataException(Exception):
    def __init__(self, message):
        super().__init__(message)


class WrongSettlementException(Exception):
    def __init__(self, message):
        super().__init__(message)


class NoAlarmsException(Exception):
    def __init__(self, message):
        super().__init__(message)