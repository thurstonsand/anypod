"""Typed helpers for FastAPI test clients."""

from collections.abc import Callable
from typing import ClassVar, Protocol, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient as FastAPITestClient
from httpx import Response


class ClientProtocol(Protocol):
    """Protocol for the TestClient operations exercised by tests."""

    __test__: ClassVar[bool]
    app: FastAPI
    get: Callable[..., Response]
    post: Callable[..., Response]
    delete: Callable[..., Response]
    head: Callable[..., Response]
    close: Callable[[], None]


ClientProtocol.__test__ = False


def create_test_client(app: FastAPI) -> ClientProtocol:
    """Create a FastAPI test client with stable public typing."""
    return cast(ClientProtocol, FastAPITestClient(app))
