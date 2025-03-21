from collections.abc import AsyncIterable, AsyncIterator

_NOT_PROVIDED = object()  # sentinel object to detect when a kwarg was not given

# Async Helper functions
def aiter(obj, sentinel=_NOT_PROVIDED):
    """aiter(async_iterable) -> async_iterator
    aiter(async_callable, sentinel) -> async_iterator
    Like the iter() builtin but for async iterables and callables.

    source: https://stackoverflow.com/a/64081451/3776765
    """
    if sentinel is _NOT_PROVIDED:
        if not isinstance(obj, AsyncIterable):
            raise TypeError(f'aiter expected an AsyncIterable, got {type(obj)}')
        if isinstance(obj, AsyncIterator):
            return obj
        return (i async for i in obj)

    if not callable(obj):
        raise TypeError(f'aiter expected an async callable, got {type(obj)}')

    async def ait():
        while True:
            value = await obj()
            if value == sentinel:
                break
            yield value

    return ait()

def anext(async_iterator):
    if not hasattr(async_iterator, "__anext__"):
        raise TypeError(f"'{type(async_iterator)}' object is not an awaitable iterator")

    return async_iterator.__anext__()
