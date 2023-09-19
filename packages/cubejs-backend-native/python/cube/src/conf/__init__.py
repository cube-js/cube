import os
from typing import Union, Callable, Dict, Any


def file_repository(path):
    files = []

    for (dirpath, dirnames, filenames) in os.walk(path):
        for fileName in filenames:
            if fileName.endswith(".js") or fileName.endswith(".yml") or fileName.endswith(".yaml") or fileName.endswith(".jinja") or fileName.endswith(".py"):
                path = os.path.join(dirpath, fileName)

                f = open(path, 'r')
                content = f.read()
                f.close()

                files.append({
                    'fileName': fileName,
                    'content': content
                })

    return files

class ConfigurationException(Exception):
    pass

class RequestContext:
    url: str
    method: str
    headers: dict[str, str]


class Configuration:
    web_sockets: bool
    http: Dict
    graceful_shutdown: int
    process_subscriptions_interval: int
    web_sockets_base_path: str
    schema_path: str
    base_path: str
    dev_server: bool
    api_secret: str
    cache_and_queue_driver: str
    allow_js_duplicate_props_in_schema: bool
    jwt: Dict
    scheduled_refresh_timer: Any
    scheduled_refresh_timezones: list[str]
    scheduled_refresh_concurrency: int
    scheduled_refresh_batch_size: int
    compiler_cache_size: int
    update_compiler_cache_keep_alive: bool
    max_compiler_cache_keep_alive: int
    telemetry: bool
    sql_cache: bool
    live_preview: bool
    # SQL API
    pg_sql_port: int
    sql_super_user: str
    sql_user: str
    sql_password: str
    # Functions
    logger: Callable
    context_to_app_id: Union[str, Callable[[RequestContext], str]]
    context_to_orchestrator_id: Union[str, Callable[[RequestContext], str]]
    driver_factory: Callable[[RequestContext], Dict]
    external_driver_factory: Callable[[RequestContext], Dict]
    db_type: Union[str, Callable[[RequestContext], str]]
    check_auth: Callable
    check_sql_auth: Callable
    can_switch_sql_user: Callable
    extend_context: Callable
    scheduled_refresh_contexts: Callable
    context_to_api_scopes: Callable
    repository_factory: Callable
    schema_version: Union[str, Callable[[RequestContext], str]]
    semantic_layer_sync: Union[Dict, Callable[[], Dict]]
    pre_aggregations_schema: Union[Callable[[RequestContext], str]]
    orchestrator_options: Union[Dict, Callable[[RequestContext], Dict]]

    def __init__(self):
        self.web_sockets = None
        self.http = None
        self.graceful_shutdown = None
        self.schema_path = None
        self.base_path = None
        self.dev_server = None
        self.api_secret = None
        self.web_sockets_base_path = None
        self.pg_sql_port = None
        self.cache_and_queue_driver = None
        self.allow_js_duplicate_props_in_schema = None
        self.process_subscriptions_interval = None
        self.jwt = None
        self.scheduled_refresh_timer = None
        self.scheduled_refresh_timezones = None
        self.scheduled_refresh_concurrency = None
        self.scheduled_refresh_batch_size = None
        self.compiler_cache_size = None
        self.update_compiler_cache_keep_alive = None
        self.max_compiler_cache_keep_alive = None
        self.telemetry = None
        self.sql_cache = None
        self.live_preview = None
        self.sql_super_user = None
        self.sql_user = None
        self.sql_password = None
        # Functions
        self.logger = None
        self.context_to_app_id = None
        self.context_to_orchestrator_id = None
        self.driver_factory = None
        self.external_driver_factory = None
        self.db_type = None
        self.check_auth = None
        self.check_sql_auth = None
        self.can_switch_sql_user = None
        self.query_rewrite = None
        self.extend_context = None
        self.scheduled_refresh_contexts = None
        self.context_to_api_scopes = None
        self.repository_factory = None
        self.schema_version = None
        self.semantic_layer_sync = None
        self.pre_aggregations_schema = None
        self.orchestrator_options = None

    def __call__(self, func):
        if not callable(func):
            raise ConfigurationException("@config decorator must be used with functions, actual: '%s'" % type(func).__name__)

        if hasattr(self, func.__name__):
            setattr(self, func.__name__, func)
        else:
            raise ConfigurationException("Unknown configuration property: '%s'" % func.__name__)

config = Configuration()
# backward compatibility
settings = config