from typing import Any, List, Optional, Tuple

from requests import Session

from .model import Entry, Project, Version, VersionRelease

DEFAULT_REQUEST_TIMEOUT = 5.0


class Client:
    def __init__(self, api_endpoint: str, api_key: str):
        from . import __version__ as version

        self._http = Session()
        self._default_headers = {
            "User-Agent": f"config-center-sdk-python/{version}",
            "X-Config-Center-API-Key": api_key,
        }
        self._endpoint = api_endpoint

    def get_project(self, name: str) -> Project:
        data = self._get_data(f"projects/{name}")
        return Project.from_dict(data)

    def list_active_entries(self, project_id: str, env: str) -> Tuple[Optional[Version], List[Entry]]:
        data = self._get_data(f"/projects/{project_id}/{env}/entries")
        version = None
        entries = [Entry.from_dict(item) for item in data["items"]]

        if data["version"]:
            version = Version.from_dict(data["version"])

        return version, entries

    def watch(
        self,
        project_id: str,
        env: str,
        last_version: Optional[str] = None,
        timeout_seconds: int = 60,
    ) -> VersionRelease:
        params = {
            "lastVersion": last_version,
            "timeout": f"{timeout_seconds}s",
        }
        data = self._get_data(f"/projects/{project_id}/{env}/watch", params=params)
        return VersionRelease.from_dict(data)

    def _get_data(self, path: str, **kwargs: Any) -> dict:
        _, _, resp = self._get(path, **kwargs)
        return resp["data"]

    def _get(self, path: str, **kwargs: Any) -> Tuple[int, dict, Any]:
        return self._request("GET", path, **kwargs)

    def _request(
        self,
        method: str,
        path: str,
        expect_json_resp: bool = True,
        **kwargs: Any,
    ) -> Tuple[int, Any, Any]:
        if path.startswith("/"):
            path = path[1:]

        url = f"{self._endpoint}/{path}"
        headers = kwargs.pop("headers", {}).copy()
        headers.update(self._default_headers)
        timeout = kwargs.pop("timeout", DEFAULT_REQUEST_TIMEOUT)

        resp = self._http.request(method, url, headers=headers, timeout=timeout, **kwargs)
        resp.raise_for_status()

        body = resp.json() if expect_json_resp else resp.text
        return resp.status_code, resp.headers, body
