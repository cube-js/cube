from cube import TemplateContext
import os
from utils import answer_to_main_question


template = TemplateContext()

value_or_none = os.getenv('MY_ENV_VAR')
template.add_variable('value_or_none', value_or_none)

value_or_default = os.getenv('MY_OTHER_ENV_VAR', 'my_default_value')
template.add_variable('value_or_default', value_or_default)

template.add_variable('answer_to_main_question', answer_to_main_question())
