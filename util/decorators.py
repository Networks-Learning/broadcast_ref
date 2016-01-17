def cache_enabled(f):
    def wrapper(self, *args, **kwargs):
        attr = '_%s__%s' % (f.__name__, '_'.join([str(a) for a in args if a is not None]))
        if not hasattr(self, attr) or getattr(self, attr) is None:
            setattr(self, attr, f(self, *args, **kwargs))

        return getattr(self, attr)

    return wrapper
