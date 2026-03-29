"""
Strategy REST API authentication module.

Handles login/logout and session token management.
All scripts should use this instead of managing auth directly.
"""

import requests
from core.config import get_config


class StrategySession:
    """Manages an authenticated session with the Strategy REST API."""

    def __init__(self):
        self.config = get_config()
        self.base_url = self.config["base_url"]
        self.verify_ssl = self.config["verify_ssl"]
        self.auth_token = None
        self.cookies = None

    def login(self):
        """Authenticate and store the session token."""
        url = f"{self.base_url}/auth/login"
        body = {
            "username": self.config["username"],
            "password": self.config["password"],
        }
        resp = requests.post(url, json=body, verify=self.verify_ssl)
        resp.raise_for_status()

        self.auth_token = resp.headers.get("X-MSTR-AuthToken")
        self.cookies = resp.cookies
        if not self.auth_token:
            raise RuntimeError("Login succeeded but no auth token received.")

        print(f"Logged in as {self.config['username']}")
        return self

    def logout(self):
        """Close the authenticated session."""
        if not self.auth_token:
            return
        url = f"{self.base_url}/auth/logout"
        requests.post(url, headers=self._headers(), cookies=self.cookies, verify=self.verify_ssl)
        self.auth_token = None
        self.cookies = None
        print("Logged out.")

    def get(self, endpoint, params=None):
        """Make an authenticated GET request."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = requests.get(url, headers=self._headers(), cookies=self.cookies,
                            params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp.json()

    def post(self, endpoint, json=None, params=None):
        """Make an authenticated POST request."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = requests.post(url, headers=self._headers(), cookies=self.cookies,
                             json=json, params=params, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp

    def put(self, endpoint, json=None):
        """Make an authenticated PUT request."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = requests.put(url, headers=self._headers(), cookies=self.cookies,
                            json=json, verify=self.verify_ssl)
        resp.raise_for_status()
        return resp

    def delete(self, endpoint):
        """Make an authenticated DELETE request."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = requests.delete(url, headers=self._headers(), cookies=self.cookies,
                               verify=self.verify_ssl)
        resp.raise_for_status()
        return resp

    def _headers(self):
        """Return auth headers for API requests."""
        h = {"X-MSTR-AuthToken": self.auth_token}
        project_id = self.config.get("project_id")
        if project_id:
            h["X-MSTR-ProjectID"] = project_id
        return h

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, *args):
        self.logout()
