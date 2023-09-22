class JinjaException(Exception):
    pass

class JinjaContext:
    def function(self, func):
        if not callable(func):
            raise JinjaException("function registration must be used with functions, actual: '%s'" % type(func).__name__)
    
        return context_func(func)

    def filter(self, func):
        if not callable(func):
            raise JinjaException("function registration must be used with functions, actual: '%s'" % type(func).__name__)

        raise JinjaException("filter registration is not supported")

    def variable(self, func):
        raise JinjaException("variable registration is not supported")

class TemplateModule:
    def JinjaContext():
        return JinjaContext()

template = TemplateModule

def context_func(func):
    func.cube_context_func = True
    return func

__all__ = [
    'context_func',
    'template'
]
