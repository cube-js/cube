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
    scheduled_refresh_time_zones: Union[Callable[[RequestContext], list[str]], list[str]]
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
    context_to_cube_store_router_id: Union[str, Callable[[RequestContext], str]]
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
    pre_aggregations_schema: Union[Callable[[RequestContext], str], str]
    orchestrator_options: Union[Dict, Callable[[RequestContext], Dict]]
    context_to_roles: Callable[[RequestContext], list[str]]
    context_to_groups: Callable[[RequestContext], list[str]]
    fast_reload: bool

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
        self.context_to_cube_store_router_id = None
        self.driver_factory = None
        self.external_driver_factory = None
        self.db_type = None
        self.check_auth = None
        self.check_sql_auth = None
        self.can_switch_sql_user = None
        self.query_rewrite = None
        self.extend_context = None
        self.scheduled_refresh_contexts = None
        self.scheduled_refresh_time_zones = None
        self.context_to_api_scopes = None
        self.repository_factory = None
        self.schema_version = None
        self.semantic_layer_sync = None
        self.pre_aggregations_schema = None
        self.orchestrator_options = None
        self.context_to_roles = None
        self.context_to_groups = None
        self.fast_reload = None

    def __call__(self, func):
        if isinstance(func, str):
            return AttrRef(self, func)

        if not callable(func):
            raise ConfigurationException("@config decorator must be used with functions, actual: '%s'" % type(func).__name__)

        if hasattr(self, func.__name__):
            setattr(self, func.__name__, func)
        else:
            raise ConfigurationException("Unknown configuration property: '%s'" % func.__name__)

class AttrRef:
    config: Configuration
    attribute: str

    def __init__(self, config: Configuration, attribute: str):
        self.config = config
        self.attribute = attribute

    def __call__(self, func):
        if not callable(func):
            raise ConfigurationException("@config decorator must be used with functions, actual: '%s'" % type(func).__name__)

        if hasattr(self.config, self.attribute):
            setattr(self.config, self.attribute, func)
        else:
            raise ConfigurationException("Unknown configuration property: '%s'" % func.__name__)

        return func

config = Configuration()
# backward compatibility
settings = config

class TemplateException(Exception):
    pass

class TemplateContext:
    functions: dict[str, Callable]
    variables: dict[str, Any]
    filters: dict[str, Callable]

    def __init__(self):
        self.functions = {}
        self.variables = {}
        self.filters = {}

    def add_function(self, name, func):
        if not callable(func):
            raise TemplateException("function registration must be used with functions, actual: '%s'" % type(func).__name__)

        self.functions[name] = func

    def add_variable(self, name, val):
        if name in self.functions:
            raise TemplateException("unable to register variable: name '%s' is already in use for function" % name)

        self.variables[name] = val

    def add_filter(self, name, func):
        if not callable(func):
            raise TemplateException("function registration must be used with functions, actual: '%s'" % type(func).__name__)

        self.filters[name] = func

    def function(self, func):
        if isinstance(func, str):
            return TemplateFunctionRef(self, func)

        self.add_function(func.__name__, func)
        return func

    def filter(self, func):
        if isinstance(func, str):
            return TemplateFilterRef(self, func)

        self.add_filter(func.__name__, func)
        return func

class TemplateFunctionRef:
    context: TemplateContext
    attribute: str

    def __init__(self, context: TemplateContext, attribute: str):
        self.context = context
        self.attribute = attribute

    def __call__(self, func):
        self.context.add_function(self.attribute, func)
        return func


class TemplateFilterRef:
    context: TemplateContext
    attribute: str

    def __init__(self, context: TemplateContext, attribute: str):
        self.context = context
        self.attribute = attribute

    def __call__(self, func):
        self.context.add_filter(self.attribute, func)
        return func

def context_func(func):
    func.cube_context_func = True
    return func

class SafeString(str):
    is_safe: bool

    def __init__(self, v: str):
        self.is_safe = True

__all__ = [
    'context_func',
    'TemplateContext',
    'SafeString',
]
