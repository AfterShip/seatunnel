from logging import getLogger
from typing import Optional

from .client import Client
from .model import ErrorSecretConcealed, Version
from .option import Options

logger = getLogger(__name__)


class ConfigLoader:
    client: Client
    debug: bool
    project_name: str
    project_id: Optional[str]
    env: str
    version: Optional[Version]

    @classmethod
    def default(cls) -> "ConfigLoader":
        """
        Init `ConfigLoader` with default options inferred from envs.
        """

        opts = Options.from_envs()
        opts.validate()
        logger.info("loaded client options from env: %s", opts)

        client = Client(opts.api_endpoint, opts.api_key)
        return cls(client, opts.project, opts.env, debug=opts.debug)

    def __init__(self, client: Client, project: str, env: str, debug: bool = False):
        self.client = client
        self.project_name = project
        self.project_id = None
        self.env = env
        self.version = None
        self.debug = debug

    def load(self) -> dict:
        """
        Load current active config from Config Center and cache the version.
        """

        if self.env == "local":
            return {}

        if not self.project_id:
            project = self.client.get_project(self.project_name)
            self.project_id = project.id

        version, entries = self.client.list_active_entries(self.project_id, self.env)
        config = {}

        for entry in entries:
            try:
                config[entry.key] = entry.value
            except ErrorSecretConcealed:
                if self.debug:
                    config[entry.key] = None
                    continue
                raise

        if version:
            self.version = version
            logger.info("loaded active version %s with keys: %s", version.id, ", ".join(config.keys()))

        return config
