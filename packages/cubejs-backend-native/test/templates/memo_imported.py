from datetime import date

from cube import TemplateContext
from memo_helper import Cfg, Client, api, load_day, load_imported

template = TemplateContext()
template.add_function('load_imported', load_imported)


@template.function
def via_python(name):
    # Calls the memoized helper directly, not through the template
    return load_imported(name)


@template.function
def get_api():
    return api


@template.function
def client_tables(schema):
    client = Client(schema)
    return client.tables() * 10 + client.tables()


@template.function
def client_calls():
    return len(Client.calls)


@template.function
def day():
    # A new but equal date on every call
    return load_day(date(2024, 1, 1))


@template.function
def cfg():
    # Equal but not hashable
    return load_day(Cfg('a'))
