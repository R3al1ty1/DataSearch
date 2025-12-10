class DataSearchBaseException(Exception):
    """Base exception for DataSearch application."""
    pass


class ResourceNotFound(DataSearchBaseException):
    def __init__(self, resource: str, identifier: str):
        self.message = f"{resource} with id '{identifier}' not found."
        super().__init__(self.message)


class ExternalServiceError(DataSearchBaseException):
    def __init__(self, service: str, details: str):
        self.message = f"Error communicating with {service}: {details}"
        super().__init__(self.message)


class InvalidSearchQuery(DataSearchBaseException):
    pass
