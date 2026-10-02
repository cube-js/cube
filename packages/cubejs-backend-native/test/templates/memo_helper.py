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
