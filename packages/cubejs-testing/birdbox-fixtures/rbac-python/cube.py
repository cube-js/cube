# Cube configuration options: https://cube.dev/docs/config

from cube import config


@config('context_to_roles')
def context_to_roles(context):
    return context.get("securityContext", {}).get("auth", {}).get("roles", [])


def extract_matching_dicts(data):
    matching_dicts = []
    keys = ['values', 'member', 'operator']

    # Recursive function to traverse through the list or dictionary
    def traverse(element):
        if isinstance(element, dict):
            # Check if any of the specified keys are in the dictionary
            if any(key in element for key in keys):
                matching_dicts.append(element)
            # Traverse the dictionary values
            for value in element.values():
                traverse(value)
        elif isinstance(element, list):
            # Traverse the list items
            for item in element:
                traverse(item)

    traverse(data)
    return matching_dicts


@config('query_rewrite')
def query_rewrite(query: dict, ctx: dict) -> dict:
    filters = extract_matching_dicts(query.get('filters'))

    for value in range(len(query['timeDimensions'])):
        filters.append(query['timeDimensions'][value]['dateRange'])

    if not filters or None in filters:
        raise Exception("Queries can't be run without a filter")
    return query


@config('check_sql_auth')
def check_sql_auth(query: dict, username: str, password: str) -> dict:
    if username == 'admin':
        return {
            'username': 'admin',
            'password': password,
            'securityContext': {
                'auth': {
                    'username': 'admin',
                    'userAttributes': {
                        'canHaveAdmin': True,
                        'city': 'New York'
                    },
                    'roles': ['admin']
                }
            }
        }
    raise Exception("Invalid username or password")
