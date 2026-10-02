from cube import memo

# Imported modules load once per process, unlike globals.py
calls = []


@memo
def load_imported(name):
    calls.append(name)
    return len(calls)
