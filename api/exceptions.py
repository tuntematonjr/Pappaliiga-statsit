from __future__ import annotations


class ServiceError(RuntimeError):
    """Base class for service layer errors."""

    status_code = 500


class NotFoundError(ServiceError):
    """Raised when a requested resource is not present."""

    status_code = 404


class BadRequestError(ServiceError):
    """Raised when request validation fails at the service layer."""

    status_code = 400
