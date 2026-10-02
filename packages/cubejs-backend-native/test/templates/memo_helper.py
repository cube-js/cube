from dataclasses import dataclass

from cube import memo

# Imported modules load once per process, unlike globals.py
calls = []


@memo
def load_imported(name):
    calls.append(name)
    return len(calls)


class Api:
    calls = []

    # Called from a template on an object a template function returned, after that function
    # returned: there's no compilation to cache for, so it runs every time
    @memo
    def tables(self, name):
        Api.calls.append(name)
        return len(Api.calls)


api = Api()


class Client:
    calls = []

    def __init__(self, schema):
        self.schema = schema

    # Each instance is its own argument: a freed instance's address may well go to the next one
    @memo
    def tables(self):
        Client.calls.append(self.schema)
        return {'a': 1, 'b': 2}[self.schema]


days = []


@memo
def load_day(day):
    days.append(day)
    return len(days)


@dataclass
class Cfg:
    name: str
