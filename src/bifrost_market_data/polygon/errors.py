"""Polygon REST client exceptions."""

from __future__ import annotations


class PolygonAPIError(Exception):
    """Non-retryable or exhausted Polygon API failure."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str = "",
        body: object | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.url = url
        self.body = body

    def __str__(self) -> str:
        code = f"HTTP {self.status_code}" if self.status_code is not None else "no status"
        if self.url:
            return f"{self.message} ({code}, url={self.url})"
        return f"{self.message} ({code})"


class PolygonRateLimitError(PolygonAPIError):
    """HTTP 429 — caller may retry after ``retry_after`` seconds."""

    def __init__(
        self,
        message: str = "rate limited",
        *,
        retry_after: float | None = None,
        status_code: int = 429,
        url: str = "",
        body: object | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code, url=url, body=body)
        self.retry_after = retry_after
