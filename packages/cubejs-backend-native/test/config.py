from cube.conf import (
    config,
    settings,
    file_repository
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
async def repository_factory(ctx):
    print('[python] repository_factory ctx=', ctx)

    return file_repository(ctx['securityContext']['schemaPath'])

@config
async def context_to_api_scopes():
    print('[python] context_to_api_scopes')
    return ['meta', 'data', 'jobs']
