# Dynamic data models: https://docs.cube.dev/docs/data-modeling/dynamic

from cube import TemplateContext

template = TemplateContext()

# Python functions can be registered so that they are callable from Jinja.
# See '{{ is_accessible_by_team(...) }}' in `views/customers.yml'.
# Learn more: https://docs.cube.dev/docs/data-modeling/dynamic/jinja
@template.function('is_accessible_by_team')
def is_accessible_by_team(team: str, ctx: dict) -> bool:
  return team == ctx['securityContext'].setdefault('team', 'default')
