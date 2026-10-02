from cube import TemplateContext, memo

template = TemplateContext()
calls = {'sync': 0, 'async': 0}


@template.function
@memo
def load_sync(name):
    calls['sync'] += 1
    return name + '_' + str(calls['sync'])


@template.function('load_async')
@memo
async def load_async_data(name):
    calls['async'] += 1
    return name + '_' + str(calls['async'])
