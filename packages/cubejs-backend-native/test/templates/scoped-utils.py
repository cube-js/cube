source_code = """
from cube import (context_func, SafeString)

@context_func
def arg_sum_integers(a, b):
  return a + b

@context_func
def arg_bool(a):
  return a + 0

@context_func
def arg_str(a):
  return a

@context_func
def arg_null(a):
  return a

@context_func
def arg_sum_tuple(tu):
  return tu[0] + tu[1]

@context_func
def arg_sum_map(obj):
  return obj['field_a'] + obj['field_b']

@context_func
def arg_kwargs(**kwargs):
    kwargs_str = ",".join(f"{key}={value}" for key, value in sorted(kwargs.items()))

    return "arg1: " + arg1 + ", arg2: " + arg2 + ", kwarg:(" + kwargs_str + ")"

@context_func
def arg_named_arguments(arg1, arg2):
    return "arg1: " + arg1 + ", arg2: " + arg2

@context_func
def arg_seq(a):
  return a

@context_func
def new_int_tuple():
  return (1,2)

@context_func
def new_str_tuple():
  return ("hello", "word")

@context_func
def new_safe_string():
  return SafeString('"safe string" <>')

class MyCustomObject(dict):
  def __init__(self):
    self['a_attr'] = "value for attribute a"
# TODO: We need stable sort for dump
#     self['b_attr'] = "value for attribute b"

@context_func
def new_object_from_dict():
  return MyCustomObject()

@context_func
def load_data_sync():
   client = MyApiClient("google.com")
   return client.load_data()

@context_func
async def load_data():
    api_response = {
      "cubes": [
        {
          "name": "cube_from_api",
          "measures": [
            { "name": "count", "type": "count" },
            { "name": "total", "type": "sum", "sql": "amount" }
          ],
          "dimensions": []
        },
        {
          "name": "cube_from_api_with_dimensions",
          "measures": [
            { "name": "active_users", "type": "count_distinct", "sql": "user_id" }
          ],
          "dimensions": [
            { "name": "city", "sql": "city_column", "type": "string" }
          ]
        }
      ]
    }
    return api_response

class ExampleClassModelB:
  def get_name_method(self):
    return "example"

@context_func
def load_class_model():
  return ExampleClassModelB()

@context_func
def throw_exception():
    raise Exception('Random Exception')
"""

__execution_context_globals = {}
__execution_context_locals = {}

exec(source_code, __execution_context_globals, __execution_context_locals)
