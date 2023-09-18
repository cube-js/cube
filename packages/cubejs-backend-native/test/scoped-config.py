source_code = """
from cube.conf import (
    settings,
    config
)

settings.schema_path = "models"
settings.pg_sql_port = 5555
settings.telemetry = False

@config
def query_rewrite(query, ctx):
    print('[python] query_rewrite query=', query, ' ctx=', ctx)
    return query

@config
async def check_auth(req, authorization):
    print('[python] check_auth req=', req, ' authorization=', authorization)

@config
async def context_to_api_scopes():
    print('[python] context_to_api_scopes')
    return ['meta', 'data', 'jobs']
"""

__execution_context_globals = {}
__execution_context_locals = {}

exec(source_code, __execution_context_globals, __execution_context_locals)
