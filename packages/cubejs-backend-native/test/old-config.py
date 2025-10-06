from cube import (
    settings,
    file_repository
)

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

async def repository_factory(ctx):
    print('[python] repository_factory ctx=', ctx)

    return file_repository(ctx['securityContext']['schemaPath'])

settings.repository_factory = repository_factory

async def context_to_api_scopes():
    print('[python] context_to_api_scopes')
    return ['meta', 'data', 'jobs']

settings.context_to_api_scopes = context_to_api_scopes

async def scheduled_refresh_time_zones(ctx):
  print("[python] scheduled_refresh_time_zones ctx=", ctx)
  return ["Europe/Kyiv", "Antarctica/Troll", "Australia/Sydney"]

settings.scheduled_refresh_time_zones = scheduled_refresh_time_zones


async def scheduled_refresh_contexts(ctx):
  print("[python] scheduled_refresh_contexts ctx=", ctx)
  return [
    {
      "securityContext": {
        "appid": 'test1', "u": {"prop1": "value1"}
      }
    },
    {
      "securityContext": {
        "appid": 'test2', "u": {"prop1": "value2"}
      }
    },
    {
      "securityContext": {
        "appid": 'test3', "u": {"prop1": "value3"}
      }
    },
  ]

settings.scheduled_refresh_contexts = scheduled_refresh_contexts

def schema_version(ctx):
    print('[python] schema_version', ctx)

    return '1'

settings.schema_version = schema_version

def pre_aggregations_schema(ctx):
    print('[python] pre_aggregations_schema', ctx)

    return 'schema'

settings.pre_aggregations_schema = pre_aggregations_schema

def logger(msg, params):
    print('[python] logger msg', msg, 'params=', params)

settings.logger = logger

def context_to_roles(ctx):
    print('[python] context_to_roles', ctx)
    return ['admin']

settings.context_to_roles = context_to_roles

def context_to_groups(ctx):
    print('[python] context_to_groups', ctx)
    return ['dev', 'analytics']

settings.context_to_groups = context_to_groups
