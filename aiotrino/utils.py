# Async Helper functions
def aiter(async_iterable):
    if not hasattr(async_iterable, '__aiter__'):
        raise TypeError(f"'{type(async_iterable)}' object is not an async iterable")

    return async_iterable.__aiter__()


def anext(async_iterator):
    if not hasattr(async_iterator, '__anext__'):
        raise TypeError(f"'{type(async_iterator)}' object is not an awaitable iterator")

    return async_iterator.__anext__()
