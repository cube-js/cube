from cube import config, file_repository
from utils import test_function

config.schema_path = "models"
config.pg_sql_port = 5555
config.telemetry = False


@config
def query_rewrite(query, ctx):
    query = test_function(query)
    print("[python] query_rewrite query=", query, " ctx=", ctx)
    return query


@config
async def check_auth(req, authorization):
    print("[python] check_auth req=", req, " authorization=", authorization)
    return {
        "security_context": {"sub": "1234567890", "iat": 1516239022, "user_id": 42},
        "ignoredField": "should not be visible",
    }


@config('extend_context')
def extend_context(req):
  print("[python] extend_context req=", req)
  if "securityContext" not in req:
    return {
      "security_context": {
        "error": "missing",
      }
    }

  req["securityContext"]["extended_by_config"] = True

  return {
    "security_context": req["securityContext"],
  }


@config
async def repository_factory(ctx):
    print("[python] repository_factory ctx=", ctx)

    return file_repository(ctx["securityContext"]["schemaPath"])


@config
async def context_to_api_scopes():
    print("[python] context_to_api_scopes")
    return ["meta", "data", "jobs"]


@config
async def scheduled_refresh_time_zones(ctx):
    print("[python] scheduled_refresh_time_zones ctx=", ctx)
    return ["Europe/Kyiv", "Antarctica/Troll", "Australia/Sydney"]


@config
async def scheduled_refresh_contexts(ctx):
    print("[python] scheduled_refresh_contexts ctx=", ctx)
    return [
      {
        "securityContext": {
          "appid": 'test1', "u": { "prop1": "value1" }
        }
      },
      {
        "securityContext": {
          "appid": 'test2', "u": { "prop1": "value2" }
        }
      },
      {
        "securityContext": {
          "appid": 'test3', "u": { "prop1": "value3" }
        }
      },
    ]


@config
def schema_version(ctx):
    print("[python] schema_version", ctx)

    return "1"


@config
def pre_aggregations_schema(ctx):
    print("[python] pre_aggregations_schema", ctx)

    return "schema"


@config
def logger(msg, params):
    print("[python] logger msg", msg, "params=", params)


@config
def context_to_roles(ctx):
    print("[python] context_to_roles", ctx)

    return [
        "admin",
    ]


@config
def context_to_groups(ctx):
    print("[python] context_to_groups", ctx)

    return [
        "dev",
        "analytics",
    ]
