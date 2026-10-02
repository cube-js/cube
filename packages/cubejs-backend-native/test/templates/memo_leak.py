import asyncio
import gc
import weakref

from cube import TemplateContext, memo

template = TemplateContext()


@memo
async def load(name):
    await asyncio.sleep(0)
    return name


@memo
def load_sync(name):
    return name


@memo
async def fail(name):
    await asyncio.sleep(0)
    raise ValueError(name)


@memo
def fail_sync(name):
    raise ValueError(name)


@template.function
def contexts_kept():
    # Calls each memoized function through three short-lived TemplateContexts, as compilations
    # would, then counts how many of the contexts their caches still keep alive
    refs = []
    for fn in (load, load_sync, fail, fail_sync):
        for _ in range(3):
            context = TemplateContext()
            context.add_function('fn', fn)
            try:
                result = context.functions['fn']('a')
                if asyncio.iscoroutine(result):
                    asyncio.run(result)
            except ValueError:
                # A cached exception's traceback leads back to the context too
                pass
            refs.append(weakref.ref(context))
            del context
    gc.collect()
    return sum(ref() is not None for ref in refs)
