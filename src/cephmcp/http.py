"""HTTP access to metric endpoints (exporters and the Prometheus API)."""

from __future__ import annotations

from typing import Any

import httpx2

from .config import Settings


class HttpError(Exception):
    """An HTTP request failed. The message is safe to show to the model."""


class Http:
    """A shared async HTTP client.

    Proxy settings from the environment are ignored on purpose: exporters and
    Prometheus live on the cluster network, which a site proxy cannot reach.
    """

    def __init__(self, settings: Settings, transport: httpx2.AsyncBaseTransport | None = None):
        self._settings = settings
        self._transport = transport
        self._client: httpx2.AsyncClient | None = None

    def _get_client(self) -> httpx2.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx2.AsyncClient(
                verify=self._settings.tls_verify,
                timeout=self._settings.http_timeout,
                follow_redirects=True,
                trust_env=False,
                transport=self._transport,
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        auth: tuple[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response:
        try:
            response = await self._get_client().request(
                method,
                url,
                params=params,
                data=data,
                auth=auth,
                timeout=timeout if timeout is not None else self._settings.http_timeout,
            )
        except httpx2.TimeoutException:
            raise HttpError(f"timed out connecting to {url}") from None
        except httpx2.HTTPError as exc:
            raise HttpError(f"cannot reach {url}: {exc or type(exc).__name__}") from None
        return response

    async def get_text(self, url: str, *, timeout: float | None = None) -> str:
        response = await self.request("GET", url, timeout=timeout)
        if response.status_code != 200:
            raise HttpError(f"{url} returned HTTP {response.status_code}")
        return response.text
