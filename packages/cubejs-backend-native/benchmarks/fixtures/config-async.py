from cube import config, file_repository

config.schema_path = "models"
config.pg_sql_port = 5555
config.telemetry = False


@config
async def query_rewrite(query, ctx):
    # Removed print statements for benchmarking
    return query


@config
async def check_auth(req, authorization):
    # Removed print statements for benchmarking
    return {
        "security_context": {"sub": "1234567890", "iat": 1516239022, "user_id": 42},
        "ignoredField": "should not be visible",
    }


@config('extend_context')
async def extend_context(req):
  # Removed print statements for benchmarking
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
    # Removed print statements for benchmarking
    return file_repository(ctx["securityContext"]["schemaPath"])


@config
async def context_to_api_scopes():
    # Removed print statements for benchmarking
    return ["meta", "data", "jobs"]


@config
async def scheduled_refresh_time_zones(ctx):
    # Removed print statements for benchmarking
    return ["Europe/Kyiv", "Antarctica/Troll", "Australia/Sydney"]


@config
async def scheduled_refresh_contexts(ctx):
    # Removed print statements for benchmarking
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
async def schema_version(ctx):
    # Removed print statements for benchmarking
    return "1"


@config
async def pre_aggregations_schema(ctx):
    # Removed print statements for benchmarking
    return "schema"


@config
async def logger(msg, params):
    # Removed print statements for benchmarking
    pass


@config
async def context_to_roles(ctx):
    # Removed print statements for benchmarking
    return [
        "admin",
    ]


@config
async def context_to_groups(ctx):
    # Removed print statements for benchmarking
    return [
        "dev",
        "analytics",
    ]