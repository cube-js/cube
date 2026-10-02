import asyncio
import contextvars
import functools
import inspect
import json
import os
import threading
import weakref
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

        self.functions[name] = _in_template_context(self, func)

    def add_variable(self, name, val):
        if name in self.functions:
            raise TemplateException("unable to register variable: name '%s' is already in use for function" % name)

        self.variables[name] = val

    def add_filter(self, name, func):
        if not callable(func):
            raise TemplateException("function registration must be used with functions, actual: '%s'" % type(func).__name__)

        self.filters[name] = _in_template_context(self, func)

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

# Kept when the runtime runs this module again for the next compilation: calls still running hold
# tokens of it
_template_context = globals().get('_template_context') or contextvars.ContextVar('cube_template_context', default=None)


def _memo_key(args, kwargs):
    # Arguments come from Jinja as plain data; anything else is keyed by its repr
    call = [args, sorted(kwargs.items())]
    try:
        return json.dumps(call, sort_keys=True, default=repr)
    except TypeError:
        # Dict keys json can't sort or encode, e.g. {1: 'a', 'b': 2}
        return repr(call)


def memo(func):
    """Calls `func` once per set of arguments and returns that result to every later call made while a
    template function runs, with a cache per `TemplateContext`: each data model compilation creates
    one anew by loading `globals.py`. Other calls, with no compilation to cache for, just call `func`."""
    if not callable(func):
        raise TemplateException("memo must be used with functions, actual: '%s'" % type(func).__name__)

    # Exceptions are stored too: every template sees the same outcome
    per_context = weakref.WeakKeyDictionary()
    # Held only to look entries up: a call runs under the lock of its own key and context, so
    # other compilations and other arguments don't wait for it
    lock = threading.Lock()

    def results(context):
        stored = per_context.get(context)
        if stored is None:
            stored = per_context[context] = {}
        return stored

    def key_lock(context, key):
        with lock:
            stored = results(context)
            entry = stored.get(key)
            if entry is None:
                entry = stored[key] = {'lock': threading.RLock()}
            return entry

    if inspect.iscoroutinefunction(func):
        async def call(*args, **kwargs):
            # The task returns the exception instead of raising it: re-raising it from the task
            # would extend its traceback with every call before Python 3.11
            try:
                return True, await func(*args, **kwargs)
            except Exception as e:
                return False, (e, e.__traceback__)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            context = _template_context.get()
            if context is None:
                return await func(*args, **kwargs)
            key = _memo_key(args, kwargs)
            loop = asyncio.get_running_loop()
            with lock:
                stored = results(context)
                entry = stored.get(key)
                if entry is None or entry[1].cancelled() or (entry[0] is not loop and not entry[1].done()):
                    # A task, so concurrent calls on the loop share one invocation
                    entry = (loop, asyncio.ensure_future(call(*args, **kwargs)))
                    stored[key] = entry
            task = entry[1]
            # Shielded: cancelling one caller mustn't cancel the task the others share
            ok, value = task.result() if task.done() else await asyncio.shield(task)
            if not ok:
                error, tb = value
                raise error.with_traceback(tb)
            return value

        return async_wrapper

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        context = _template_context.get()
        if context is None:
            return func(*args, **kwargs)
        key = _memo_key(args, kwargs)
        entry = key_lock(context, key)
        with entry['lock']:
            if 'result' not in entry:
                try:
                    entry['result'] = (True, func(*args, **kwargs))
                except Exception as e:
                    # With its own traceback: raising the instance again would extend it each time
                    entry['result'] = (False, (e, e.__traceback__))
            ok, value = entry['result']
        if not ok:
            error, tb = value
            raise error.with_traceback(tb)
        return value

    return wrapper


def _in_template_context(context, func):
    # The TemplateContext of the compilation whose templates call `func`: memoized functions it
    # calls, also those of modules globals.py imports, cache their results per compilation
    if not inspect.isfunction(func):
        return func

    if inspect.iscoroutinefunction(func):
        # The native side leaves frames of functions with these names out of error tracebacks
        @functools.wraps(func)
        async def _cube_template_call_async(*args, **kwargs):
            token = _template_context.set(context)
            try:
                return await func(*args, **kwargs)
            finally:
                _template_context.reset(token)

        return _cube_template_call_async

    @functools.wraps(func)
    def _cube_template_call(*args, **kwargs):
        token = _template_context.set(context)
        try:
            return func(*args, **kwargs)
        finally:
            _template_context.reset(token)

    return _cube_template_call


def context_func(func):
    func.cube_context_func = True
    return func

class SafeString(str):
    is_safe: bool

    def __init__(self, v: str):
        self.is_safe = True

__all__ = [
    'context_func',
    'memo',
    'TemplateContext',
    'SafeString',
]
