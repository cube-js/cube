from cube import config


@config
def check_auth(req, authorization):
    return {
        "security_context": {
            "keys": sorted(req.keys()),
            "body": req.get("body"),
            "query": req.get("query"),
        },
    }
