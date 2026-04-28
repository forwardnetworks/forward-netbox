from dataclasses import dataclass

import httpx

from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardSyncError

try:
    from utilities.proxy import resolve_proxies
except ImportError:  # pragma: no cover - NetBox always provides this at runtime.
    resolve_proxies = None


DEFAULT_TURBOBULK_TIMEOUT_SECONDS = 30


class TurboBulkError(ForwardSyncError):
    """Raised when TurboBulk capability or execution cannot be resolved."""


@dataclass(frozen=True)
class TurboBulkCapability:
    available: bool
    reason: str
    status_code: int | None = None
    model_count: int = 0

    @property
    def usable(self):
        return self.available


class TurboBulkClient:
    """Small NetBox TurboBulk API client used for capability checks.

    The plugin still uses the native Django/Branching adapter path by default.
    This client exists so future TurboBulk execution can fail fast before
    branches are created when the server does not expose TurboBulk.
    """

    def __init__(
        self,
        *,
        base_url,
        token,
        verify=True,
        timeout=DEFAULT_TURBOBULK_TIMEOUT_SECONDS,
    ):
        self.base_url = str(base_url or "").rstrip("/")
        self.token = str(token or "").strip()
        self.verify = verify
        self.timeout = timeout
        if not self.base_url:
            raise TurboBulkError(
                "NetBox URL is required for TurboBulk capability checks."
            )
        if not self.token:
            raise TurboBulkError(
                "NetBox API token is required for TurboBulk capability checks."
            )

    def _api_url(self, path):
        return f"{self.base_url}/api/plugins/turbobulk{path}"

    def _headers(self):
        scheme = "Bearer" if self.token.startswith("nbt_") else "Token"
        return {
            "Accept": "application/json",
            "Authorization": f"{scheme} {self.token}",
            "User-Agent": "forward-netbox-turbobulk-probe/0.3.1",
        }

    def _proxy_mounts(self, url):
        if resolve_proxies is None:
            return None
        proxies = resolve_proxies(
            url=url,
            context={
                "client": self,
            },
        )
        if not proxies:
            return None

        mounts = {}
        for protocol, proxy_url in proxies.items():
            if not proxy_url:
                continue
            protocol = str(protocol).rstrip(":/")
            mounts[f"{protocol}://"] = httpx.HTTPTransport(proxy=proxy_url)
        return mounts or None

    def _request(self, method, path):
        url = self._api_url(path)
        try:
            with httpx.Client(
                timeout=self.timeout,
                verify=self.verify,
                mounts=self._proxy_mounts(url),
            ) as client:
                return client.request(method, url, headers=self._headers())
        except httpx.TimeoutException as exc:
            raise ForwardConnectivityError(
                "TurboBulk capability check timed out while connecting to NetBox."
            ) from exc
        except httpx.RequestError as exc:
            raise ForwardConnectivityError(
                f"Could not connect to NetBox TurboBulk endpoint: {exc}"
            ) from exc

    def check_capability(self):
        response = self._request("GET", "/models/")
        if response.status_code == 200:
            try:
                models = response.json()
            except ValueError:
                models = []
            return TurboBulkCapability(
                available=True,
                reason="available",
                status_code=response.status_code,
                model_count=len(models) if isinstance(models, list) else 0,
            )
        if response.status_code == 404:
            return TurboBulkCapability(
                available=False,
                reason="TurboBulk plugin endpoint was not found.",
                status_code=response.status_code,
            )
        if response.status_code in {401, 403}:
            return TurboBulkCapability(
                available=False,
                reason="NetBox API token cannot access TurboBulk.",
                status_code=response.status_code,
            )
        return TurboBulkCapability(
            available=False,
            reason=f"TurboBulk capability check returned HTTP {response.status_code}.",
            status_code=response.status_code,
        )
