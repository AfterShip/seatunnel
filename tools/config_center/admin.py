from enum import Enum
from http.server import BaseHTTPRequestHandler, HTTPServer
from logging import getLogger
from os import makedirs, path, stat
from queue import Empty, SimpleQueue
from threading import Thread
from time import time
from typing import Any, Callable, Iterable, List, Optional, Tuple
import json

from .client import Client
from .model import Entry, Project, Version

ADMIN_API_ENDPOINTS = {
    "development": {
        "us": "https://config-center.automizelyapi.me/api/v1",
    },
    "testing": {
        "us": "https://config-center.automizelyapi.me/api/v1",
    },
    "production": {
        "us": "https://config-center.automizelyapi.org/api/v1",
    },
}

BROWSER_LOGIN_URLS = {
    "development": "https://config-center.automizely.me/cli-login?port={port}",
    "testing": "https://config-center.automizely.me/cli-login?port={port}",
    "production": "https://config-center.automizely.org/cli-login?port={port}",
}

LOGGER = getLogger(__name__)


class AuthHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self) -> None:
        self.send_response(200)
        self.allow_cors()
        self.end_headers()

    def do_POST(self) -> None:
        n_bytes = int(self.headers.get("content-length", 0) or 0)
        data = json.loads(self.rfile.read(n_bytes))
        self.server.result_queue.put(data["token"])  # type: ignore

        self.send_response(200)
        self.allow_cors()
        self.end_headers()

    def allow_cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")


class LoginServer(HTTPServer):
    def __init__(self) -> None:
        super().__init__(("localhost", 0), AuthHandler)
        self.result_queue = SimpleQueue()  # type: ignore


class LoginStatus(Enum):
    PENDING = 0
    OK = 1


class LoginManager:

    JWT_EXPIRE_TIME = 3600  # 1h

    def __init__(self, env: str) -> None:
        self.env = env
        self.login_url = BROWSER_LOGIN_URLS[env]
        self.jwt_cache_path = path.expanduser(f"~/.config/config-center-jwt.{env}")

    def get_client(self, on_login_prompt: Callable) -> "AdminClient":
        token = self.ensure_logged_in(on_login_prompt)
        endpoint = ADMIN_API_ENDPOINTS[self.env]["us"]
        return AdminClient(endpoint, token)

    def ensure_logged_in(self, on_login_prompt: Callable) -> str:
        token = self.read_cached_token() or ""

        if not token:
            for status, value in self.wait_for_browser_login():
                if status == LoginStatus.PENDING:
                    on_login_prompt(value)
                elif status == LoginStatus.OK:
                    token = value

            makedirs(path.dirname(self.jwt_cache_path), mode=0o700, exist_ok=True)

            with open(self.jwt_cache_path, "w") as file:
                print(token, file=file)

        return token

    def read_cached_token(self) -> Optional[str]:
        if not path.exists(self.jwt_cache_path):
            return None

        # JWT expired
        if int(time() - stat(self.jwt_cache_path).st_mtime) >= self.JWT_EXPIRE_TIME:
            return None

        with open(self.jwt_cache_path, "r") as file:
            return file.read().strip()

    def wait_for_browser_login(self) -> Iterable[Tuple[LoginStatus, str]]:
        server = LoginServer()
        thread = Thread(name="login-server", target=server.serve_forever)
        thread.start()

        addr = "http://%s:%s" % server.server_address
        LOGGER.debug("server started, listening at %s", addr)
        yield (LoginStatus.PENDING, self.login_url.format(port=server.server_port))

        try:
            token = server.result_queue.get(timeout=60.0)
            LOGGER.debug("logged in successfully")
        except Empty:
            raise Exception("login timed out")

        server.shutdown()
        thread.join(timeout=60.0)
        LOGGER.debug("server stopped")

        yield (LoginStatus.OK, token)


class AdminClient(Client):
    def __init__(self, admin_api_endpoint: str, token: str) -> None:
        super().__init__(admin_api_endpoint, "")
        self._default_headers.pop("X-Config-Center-API-Key")
        self._default_headers["X-Config-Center-JWT"] = token

    def with_scope(self, project_id: str, env: str) -> "ScopedAdminClient":
        return ScopedAdminClient(self, project_id, env)

    def list_projects(self) -> List[Project]:
        items = self._get_data("/projects")["items"]
        return [Project.from_dict(item) for item in items]

    def list_pending_entries(self, project_id: str, env: str) -> List[Entry]:
        params = {"pending": "true"}
        data = self._get_data(f"/projects/{project_id}/{env}/entries", params=params)
        entries = [Entry.from_dict(item) for item in data["items"] if not item.get("deleted")]
        return entries

    def put_entry(self, project_id: str, env: str, entry_request: dict) -> None:
        path = f"/projects/{project_id}/{env}/entries/{entry_request['key']}"
        payload = {
            "type": entry_request["type"],
            "encoding": entry_request["encoding"],
            "value": entry_request["value"],
        }
        self._put(path, json=payload, expect_json_resp=False)

    def delete_entry(self, project_id: str, env: str, entry_key: str) -> None:
        path = f"/projects/{project_id}/{env}/entries/{entry_key}"
        self._delete(path, expect_json_resp=False)

    def _post(self, path: str, **kwargs: Any) -> tuple:
        return self._request("POST", path, **kwargs)

    def _put(self, path: str, **kwargs: Any) -> tuple:
        return self._request("PUT", path, **kwargs)

    def _delete(self, path: str, **kwargs: Any) -> tuple:
        return self._request("DELETE", path, **kwargs)


class ScopedAdminClient:
    def __init__(self, admin: AdminClient, project_id: str, env: str) -> None:
        self.admin = admin
        self.project_id = project_id
        self.env = env

    def list_pending_entries(self) -> List[Entry]:
        return self.admin.list_pending_entries(self.project_id, self.env)

    def list_active_entries(self) -> Tuple[Optional[Version], List[Entry]]:
        return self.admin.list_active_entries(self.project_id, self.env)

    def put_entry(self, entry_request: dict) -> None:
        return self.admin.put_entry(self.project_id, self.env, entry_request)

    def delete_entry(self, entry_key: str) -> None:
        return self.admin.delete_entry(self.project_id, self.env, entry_key)
