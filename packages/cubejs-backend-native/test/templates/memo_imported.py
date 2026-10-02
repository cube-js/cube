from cube import TemplateContext
from memo_helper import api, load_imported

template = TemplateContext()
template.add_function('load_imported', load_imported)


@template.function
def via_python(name):
    # Calls the memoized helper directly, not through the template
    return load_imported(name)


@template.function
def get_api():
    return api
