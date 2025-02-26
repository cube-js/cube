from cube import (TemplateContext, SafeString)

template = TemplateContext()
template.add_variable('var1', "test string")
template.add_variable('var2', True)
template.add_variable('var3', False)
template.add_variable('var4', None)
template.add_variable('var5', {'obj_key': 'val'})
template.add_variable('var6', [1,2,3,4,5,6])
template.add_variable('var7', [6,5,4,3,2,1])

@template.function
def arg_sum_integers(a, b):
  return a + b

@template.function("arg_bool")
def ab(a):
  return a + 0

@template.function
def arg_str(a):
  return a

@template.function
def arg_null(a):
  return a

@template.function
def arg_sum_tuple(tu):
  return tu[0] + tu[1]

@template.function
def arg_sum_map(obj):
  return obj['field_a'] + obj['field_b']

@template.function
def arg_kwargs(arg1, arg2, **kwargs):
    kwargs_str = ",".join(f"{key}={value}" for key, value in sorted(kwargs.items()))

    return "arg1: " + arg1 + ", arg2: " + arg2 + ", kwarg:(" + kwargs_str + ")"

@template.function
def arg_named_arguments(arg1, arg2):
    return "arg1: " + arg1 + ", arg2: " + arg2

@template.function
def arg_seq(a):
  return a

@template.function
def new_int_tuple():
  return (1,2)

@template.function
def new_str_tuple():
  return ("1", "2")

@template.function
def new_safe_string():
  return SafeString('"safe string" <>')

class MyCustomObject(dict):
  def __init__(self):
    self['a_attr'] = "value for attribute a"
# TODO: We need stable sort for dump
#     self['b_attr'] = "value for attribute b"

@template.function
def new_object_from_dict():
  return MyCustomObject()

@template.function
def load_data_sync():
  client = MyApiClient("google.com")
  return client.load_data()

@template.function
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

class ExampleClassModelA:
  def get_name_method(self):
    return "example"

@template.function
def load_class_model():
  return ExampleClassModelA()

@template.filter
def str_filter(i):
  return 'str from python'

@template.filter
def filter_return_arg(i):
  return i

@template.function
def throw_exception():
    raise Exception('Random Exception')
