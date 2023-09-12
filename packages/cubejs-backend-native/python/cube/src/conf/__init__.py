import os
from typing import Union, Callable, Dict


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


class RequestContext:
    url: str
    method: str
    headers: dict[str, str]


class Configuration:
    schema_path: str
    base_path: str
    web_sockets_base_path: str
    compiler_cache_size: int
    telemetry: bool
    pg_sql_port: int
    cache_and_queue_driver: str
    allow_js_duplicate_props_in_schema: bool
    process_subscriptions_interval: int
    # Functions
    logger: Callable
    context_to_app_id: Union[str, Callable[[RequestContext], str]]
    context_to_orchestrator_id: Union[str, Callable[[RequestContext], str]]
    driver_factory: Callable
    db_type: Union[str, Callable[[RequestContext], str]]
    check_auth: Callable
    check_sql_auth: Callable
    can_switch_sql_user: Callable
    extend_context: Callable
    scheduled_refresh_contexts: Callable
    context_to_api_scopes: Callable
    repository_factory: Callable
    schema_version: Callable[[RequestContext], str]
    semantic_layer_sync: Callable

    def __init__(self):
        self.schema_path = None
        self.base_path = None
        self.web_sockets_base_path = None
        self.compiler_cache_size = None
        self.telemetry = None
        self.pg_sql_port = None
        self.cache_and_queue_driver = None
        self.allow_js_duplicate_props_in_schema = None
        self.process_subscriptions_interval = None
        # Functions
        self.logger = None
        self.context_to_app_id = None
        self.context_to_orchestrator_id = None
        self.driver_factory = None
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

    def set_schema_path(self, schema_path: str):
        self.schema_path = schema_path

    def set_base_path(self, base_path: str):
        self.base_path = base_path

    def set_web_sockets_base_path(self, web_sockets_base_path: str):
        self.web_sockets_base_path = web_sockets_base_path

    def set_compiler_cache_size(self, compiler_cache_size: int):
        self.compiler_cache_size = compiler_cache_size

    def set_telemetry(self, telemetry: bool):
        self.telemetry = telemetry

    def set_pg_sql_port(self, pg_sql_port: int):
        self.pg_sql_port = pg_sql_port

    def set_cache_and_queue_driver(self, cache_and_queue_driver: str):
        self.cache_and_queue_driver = cache_and_queue_driver

    def set_allow_js_duplicate_props_in_schema(self, allow_js_duplicate_props_in_schema: bool):
        self.allow_js_duplicate_props_in_schema = allow_js_duplicate_props_in_schema

    def set_process_subscriptions_interval(self, process_subscriptions_interval: int):
        self.process_subscriptions_interval = process_subscriptions_interval

    def set_logger(self, logger: Callable):
        self.logger = logger

    def set_context_to_app_id(self, context_to_app_id: Union[str, Callable[[RequestContext], str]]):
        self.context_to_app_id = context_to_app_id

    def set_context_to_orchestrator_id(self, context_to_orchestrator_id: Union[str, Callable[[RequestContext], str]]):
        self.context_to_orchestrator_id = context_to_orchestrator_id

    def set_driver_factory(self, driver_factory: Callable):
        self.driver_factory = driver_factory

    def set_db_type(self, db_type: Union[str, Callable[[RequestContext], str]]):
        self.db_type = db_type

    def set_check_auth(self, check_auth: Callable):
        self.check_auth = check_auth

    def set_check_sql_auth(self, check_sql_auth: Callable):
        self.check_sql_auth = check_sql_auth

    def set_can_switch_sql_user(self, can_switch_sql_user: Callable):
        self.can_switch_sql_user = can_switch_sql_user

    def set_query_rewrite(self, query_rewrite: Callable):
        self.query_rewrite = query_rewrite

    def set_extend_context(self, extend_context: Callable[[RequestContext], Dict]):
        self.extend_context = extend_context

    def set_scheduled_refresh_contexts(self, scheduled_refresh_contexts: Callable):
        self.scheduled_refresh_contexts = scheduled_refresh_contexts

    def set_repository_factory(self, repository_factory: Callable):
        self.repository_factory = repository_factory

    def set_schema_version(self, schema_version: Callable[[RequestContext], str]):
        self.schema_version = schema_version

    def set_semantic_layer_sync(self, semantic_layer_sync: Callable):
        self.semantic_layer_sync = semantic_layer_sync


settings = Configuration()
