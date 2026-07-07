"""Authenticated MicroStrategy (Strategy One) REST client.

Endpoints verified against the vendor's own SDK (mstrio-py master) and the
"Strategy REST 2026" OpenAPI spec — see backend/MSTR_API_NOTES.md.

Session behavior:
- POST /auth/login returns the token in the X-MSTR-AuthToken response header
  plus a JSESSIONID cookie; BOTH must be replayed on every call.
- On a 401 mid-flight we re-login once and retry (service-account session
  may be expired by the Intelligence Server).
"""

import threading

import requests
import urllib3

from ..config import settings
from .errors import MstrApiError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MstrRestClient:
    def __init__(self, base_url: str | None = None, username: str | None = None,
                 password: str | None = None, verify_ssl: bool | None = None):
        self.base_url = (base_url or settings.mstr_base_url).rstrip("/")
        self.username = username or settings.mstr_username
        self.password = password or settings.mstr_password
        self.verify_ssl = settings.mstr_verify_ssl if verify_ssl is None else verify_ssl
        if not self.base_url:
            raise MstrApiError("MSTR_BASE_URL is not configured", status=500)
        self._lock = threading.Lock()
        self._session: requests.Session | None = None

    # ------------------------------------------------------------- session

    def _login(self) -> requests.Session:
        session = requests.Session()
        session.verify = self.verify_ssl
        resp = session.post(f"{self.base_url}/auth/login", json={
            "loginMode": 1,                # standard auth for the service account
            "username": self.username,
            "password": self.password,
            "applicationType": 35,         # Web client type — broadest compat
        })
        if resp.status_code not in (200, 204):
            raise MstrApiError(
                f"MSTR login failed ({resp.status_code}): {_err_text(resp)}",
                status=resp.status_code)
        token = resp.headers.get("X-MSTR-AuthToken")
        if not token:
            raise MstrApiError("MSTR login returned no X-MSTR-AuthToken", status=502)
        session.headers["X-MSTR-AuthToken"] = token
        return session

    def _ensure_session(self) -> requests.Session:
        with self._lock:
            if self._session is None:
                self._session = self._login()
            return self._session

    def _reset_session(self) -> None:
        with self._lock:
            self._session = None

    # ------------------------------------------------------------- request

    def request(self, method: str, path: str, *, project_id: str | None = None,
                params=None, json_body=None, headers=None,
                ok=(200, 201, 202, 204), _retried: bool = False):
        session = self._ensure_session()
        h = dict(headers or {})
        if project_id:
            h["X-MSTR-ProjectID"] = project_id
        resp = session.request(method, f"{self.base_url}{path}",
                               params=params, json=json_body, headers=h)
        if resp.status_code == 401 and not _retried:
            self._reset_session()
            return self.request(method, path, project_id=project_id,
                                params=params, json_body=json_body,
                                headers=headers, ok=ok, _retried=True)
        if resp.status_code not in ok:
            raise MstrApiError(
                f"{method} {path} failed ({resp.status_code}): {_err_text(resp)}",
                status=resp.status_code)
        return resp

    def get_json(self, path: str, **kw) -> dict | list:
        return self.request("GET", path, **kw).json()

    def logout(self) -> None:
        with self._lock:
            if self._session is not None:
                try:
                    self._session.post(f"{self.base_url}/auth/logout", timeout=5)
                except requests.RequestException:
                    pass
                self._session = None


def _err_text(resp) -> str:
    try:
        body = resp.json()
        return body.get("message") or body.get("code") or resp.text[:200]
    except ValueError:
        return resp.text[:200]
