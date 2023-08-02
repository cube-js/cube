source_code = """
from cube.conf import settings

settings.schema_path = "models"
settings.pg_sql_port = 5555
settings.telemetry = False

def query_rewrite(query, ctx):
    print('[python] query_rewrite query=', query, ' ctx=', ctx)
    return query

settings.query_rewrite = query_rewrite

async def check_auth(req, authorization):
    print('[python] check_auth req=', req, ' authorization=', authorization)


settings.check_auth = check_auth

async def context_to_api_scopes():
    print('[python] context_to_api_scopes')
    return ['meta', 'data', 'jobs']

settings.context_to_api_scopes = context_to_api_scopes
"""

__execution_context_globals = {}
__execution_context_locals = {}

exec(source_code, __execution_context_globals, __execution_context_locals)
