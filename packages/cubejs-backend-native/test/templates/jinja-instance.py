from cube import (TemplateContext, SafeString)

template = TemplateContext()

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
  return SafeString('"safe string"')

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
