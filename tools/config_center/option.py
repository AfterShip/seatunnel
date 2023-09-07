from dataclasses import asdict, dataclass
from logging import getLogger
from os import environ

ENV_API_ENDPOINT = "CONFIG_CENTER_ENDPOINT"
ENV_API_KEY = "CONFIG_CENTER_API_KEY"
ENV_DEBUG = "CONFIG_CENTER_DEBUG"
ENV_ENV = "CONFIG_CENTER_ENV"
ENV_ENV_ALT = "NODE_ENV"
ENV_PROJECT = "CONFIG_CENTER_PROJECT_NAME"

DEFAULT_API_ENDPOINTS = {
    "development": "http://config-center-internal.automizelyapi.me/api/v1",
    "testing": "http://config-center-internal.automizelyapi.me/api/v1",
    "staging": "http://config-center-internal.automizelyapi.org/api/v1",
    "production": "http://config-center-internal.automizelyapi.org/api/v1",
}

LOCAL_TEST_ENDPOINT = "https://config-center.automizelyapi.me/api/v1"
LOCAL_TEST_API_KEY = "FAKE-API-KEY"

logger = getLogger(__name__)


@dataclass
class Options:

    api_endpoint: str
    api_key: str
    debug: bool
    env: str
    project: str

    @classmethod
    def from_envs(cls) -> "Options":
        """
        Infer options from envs.
        """

        debug = (environ.get(ENV_DEBUG) or "") == "true"
        mappings = {
            "api_endpoint": ENV_API_ENDPOINT,
            "api_key": ENV_API_KEY,
            "project": ENV_PROJECT,
        }
        fields = {}

        for field, env in mappings.items():
            fields[field] = environ.get(env) or ""

        # whichever comes first: CONFIG_CENTER_ENV -> NODE_ENV -> "local"
        fields["env"] = environ.get(ENV_ENV) or environ.get(ENV_ENV_ALT) or "local"

        if debug:
            fields["api_endpoint"] = fields["api_endpoint"] or LOCAL_TEST_ENDPOINT
            fields["api_key"] = fields["api_key"] or LOCAL_TEST_API_KEY

            if fields["env"] == "local":
                fields["env"] = "development"

        if not fields["api_endpoint"]:
            fields["api_endpoint"] = DEFAULT_API_ENDPOINTS.get(fields["env"]) or ""

        return cls(debug=debug, **fields)

    def validate(self) -> None:
        if self.env == "local":
            return

        missing = []

        for key, value in asdict(self).items():
            if key != "debug" and not value:
                missing.append(key)

        if missing:
            raise ValueError("option fields not set: %s" % (", ".join(missing)))

    def __str__(self) -> str:
        return f"project={self.project} env={self.env} api_endpoint={self.api_endpoint} api_key=******"
