"""
Strategy REST API authentication module.

Handles login/logout and session token management.
Supports dual-server setup (PROD + DEV) via explicit base_url parameter.
"""

import urllib3
import requests

# Suppress SSL warnings globally
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class StrategySession:
    """Manages an authenticated session with the Strategy REST API."""

    def __init__(self, base_url, username, password, verify_ssl=False, project_id=None):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.project_id = project_id
        self.auth_token = None
        self.cookies = None

    @classmethod
    def from_config(cls, config, project_id=None):
        """Create session from a config dict (from config.py)."""
        return cls(
            base_url=config["base_url"],
            username=config["username"],
            password=config["password"],
            verify_ssl=config.get("verify_ssl", False),
            project_id=project_id or config.get("project_id"),
        )

    def set_project(self, project_id):
        """Switch to a different project (for multi-project iteration)."""
        self.project_id = project_id

    def login(self):
        """Authenticate and store the session token."""
        url = f"{self.base_url}/auth/login"
        body = {
            "username": self.username,
            "password": self.password,
        }
        resp = requests.post(url, json=body, verify=self.verify_ssl)
        resp.raise_for_status()

        self.auth_token = resp.headers.get("X-MSTR-AuthToken")
        self.cookies = resp.cookies
        if not self.auth_token:
            raise RuntimeError("Login succeeded but no auth token received.")

        print(f"[AUTH] Logged in as {self.username} on {self.base_url}")
        return self

    def logout(self):
        """Close the authenticated session."""
        if not self.auth_token:
            return
        url = f"{self.base_url}/auth/logout"
        try:
            requests.post(url, headers=self._headers(), cookies=self.cookies, verify=self.verify_ssl)
        except Exception:
            pass
        self.auth_token = None
        self.cookies = None
        print(f"[AUTH] Logged out from {self.base_url}")

    def get(self, endpoint, params=None, headers=None):
        """Make an authenticated GET request. Returns parsed JSON."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        h = {**self._headers(), **(headers or {})}
        resp = requests.get(url, headers=h, cookies=self.cookies,
                            params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json()

    def get_raw(self, endpoint, params=None, headers=None):
        """Make an authenticated GET request. Returns raw Response."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        h = {**self._headers(), **(headers or {})}
        resp = requests.get(url, headers=h, cookies=self.cookies,
                            params=params, verify=self.verify_ssl)
        return resp

    def post(self, endpoint, json=None, params=None, headers=None):
        """Make an authenticated POST request. Returns raw Response."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        h = {**self._headers(), **(headers or {})}
        resp = requests.post(url, headers=h, cookies=self.cookies,
                             json=json, params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp

    def put(self, endpoint, json=None, params=None, headers=None):
        """Make an authenticated PUT request. Returns raw Response."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        h = {**self._headers(), **(headers or {})}
        resp = requests.put(url, headers=h, cookies=self.cookies,
                            json=json, params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp

    def patch(self, endpoint, json=None, headers=None):
        """Make an authenticated PATCH request. Returns raw Response."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        h = {**self._headers(), **(headers or {})}
        resp = requests.patch(url, headers=h, cookies=self.cookies,
                              json=json, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp

    def delete(self, endpoint, headers=None):
        """Make an authenticated DELETE request. Returns raw Response."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        h = {**self._headers(), **(headers or {})}
        resp = requests.delete(url, headers=h, cookies=self.cookies,
                               verify=self.verify_ssl)
        resp.raise_for_status()
        return resp

    def _headers(self):
        """Return auth headers for API requests."""
        h = {"X-MSTR-AuthToken": self.auth_token}
        if self.project_id:
            h["X-MSTR-ProjectID"] = self.project_id
        return h

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, *args):
        self.logout()
