from cube import TemplateContext
from memo_helper import load_imported

template = TemplateContext()
template.add_function('load_imported', load_imported)
