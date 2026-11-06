import asyncio

from cube import TemplateContext, memo

template = TemplateContext()
calls = []


@memo
async def load(name):
    calls.append(name)
    return len(calls)


@template.function
async def load_twice(name):
    first = await load(name)
    # Another compilation loads its globals.py meanwhile
    await asyncio.sleep(0.5)
    second = await load(name)
    return first * 10 + second
