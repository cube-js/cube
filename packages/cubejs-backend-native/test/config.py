from cube import (
    config,
    file_repository
)

config.schema_path = "models"
config.pg_sql_port = 5555
config.telemetry = False

@config
def query_rewrite(query, ctx):
    print('[python] query_rewrite query=', query, ' ctx=', ctx)
    return query

@config
async def check_auth(req, authorization):
    print('[python] check_auth req=', req, ' authorization=', authorization)
    return {
      'security_context': {
        'sub': '1234567890',
        'iat': 1516239022,
        'user_id': 42
      },
      'ignoredField': 'should not be visible'
    }

@config
async def repository_factory(ctx):
    print('[python] repository_factory ctx=', ctx)

    return file_repository(ctx['securityContext']['schemaPath'])

@config
async def context_to_api_scopes():
    print('[python] context_to_api_scopes')
    return ['meta', 'data', 'jobs']

@config
def schema_version(ctx):
    print('[python] schema_version', ctx)

    return '1'

@config
def pre_aggregations_schema(ctx):
    print('[python] pre_aggregations_schema', ctx)

    return 'schema'

@config
def logger(msg, params):
    print('[python] logger msg', msg, 'params=', params)
