# import aiohttp
# import asyncio

from cube.conf import settings

settings.schema_path = "models"
settings.pg_sql_port = 5555
settings.telemetry = False

def query_rewrite(query, ctx):
    print('[python] query_rewrite query=', query, ' ctx=', ctx)
    return query

settings.query_rewrite = query_rewrite

def check_auth(req, authorization):
    print('[python] check_auth req=', req, ' authorization=', authorization)
    return req

settings.check_auth = check_auth
