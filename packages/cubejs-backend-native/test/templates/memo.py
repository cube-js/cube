from cube import TemplateContext, memo

template = TemplateContext()
calls = {'sync': 0, 'async': 0}


@template.function
@memo
def load_sync(name):
    calls['sync'] += 1
    # The call number, so a cached result shows the number of the call that made it
    return calls['sync']


@template.function('load_async')
@memo
async def load_async_data(name):
    calls['async'] += 1
    return calls['async']
