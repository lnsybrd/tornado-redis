import sys
import inspect
import logging

def name(item):
    " Return an item's name. "
    return item.__name__

def format_arg_value(arg_val):
    """
        Return a string representing a (name, value) pair.

        >>> format_arg_value(('x', (1, 2, 3)))
        'x=(1, 2, 3)'
    """
    arg, val = arg_val
    return "%s=%r" % (arg, val)

def echo(logger, fn):
    """ Echo calls to a function.

        Returns a decorated version of the input function which "echoes" calls
        made to it by writing out the function's name and the arguments it was
        called with.
    """
    import functools
    # Unpack function's arg count, arg names, arg defaults
    code = fn.func_code
    argcount = code.co_argcount
    argnames = code.co_varnames[:argcount]
    fn_defaults = fn.func_defaults or list()
    argdefs = dict(zip(argnames[-len(fn_defaults):], fn_defaults))

    @functools.wraps(fn)
    def wrapped(*v, **k):
        # Collect function arguments by chaining together positional,
        # defaulted, extra positional and keyword arguments.
        positional = map(format_arg_value, zip(argnames, v))
        defaulted = [format_arg_value((a, argdefs[a]))
        for a in argnames[len(v):] if a not in k]
        nameless = map(repr, v[argcount:])
        keyword = map(format_arg_value, k.items())
        args = positional + defaulted + nameless + keyword

        logger.debug("--> %s(%s)" % (name(fn), ", ".join(args)))
        ret = fn(*v, **k)
        logger.debug("<-- %s(%s) = %s" % (name(fn), ", ".join(args), ret))

        return ret

    return wrapped
