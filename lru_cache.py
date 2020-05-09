from collections import OrderedDict

# cache structure = [least recently used, ....... , most recently used]

class lru_cache(object):
    def __init__(self, size):
        self.size = size
        self.cache = OrderedDict()

    def __call__(self, func):
        def wrapped_f(*args):
            # if key is not in cache: send server request and add response to cache
            # else: return corresponding value from cache
            if str(args) not in self.cache:
                result = func(*args)
                self.add(str(args), result)
            else:
                result = self.get(str(args))
            return result
        return wrapped_f
    
    # gets val from cache based on key
    def get(self, key):
        if key not in self.cache:
            return None
        val = self.cache[key]
        self.cache.move_to_end(key)
        return val
    
    # adds key, val to the top of the cache
    def add(self, key, val):
        self.cache[key] = val
        self.cache.move_to_end(key)
        if len(self.cache) > self.size:
            self.cache.popitem(last=False)
