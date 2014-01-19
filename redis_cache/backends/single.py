from django.core.cache.backends.base import InvalidCacheBackendError
from django.core.exceptions import ImproperlyConfigured
from .compat import bytes_type

try:
    import redis
except ImportError:
    raise InvalidCacheBackendError("Redis cache backend requires the 'redis-py' library")

from redis_cache.backends.base import BaseRedisCache
from redis_cache.connection import pool


class RedisCache(BaseRedisCache):

    def __init__(self, server, params):
        """
        Connect to Redis, and set up cache backend.
        """
        self._init(server, params)

    def _init(self, server, params):
        super(BaseRedisCache, self).__init__(params)
        self._params = params
        self._server = server
        if not isinstance(server, bytes_type):
            self._server, = server

        self._pickle_version = None

        unix_socket_path = None
        if ':' in self._server:
            host, port = self._server.rsplit(':', 1)
            try:
                port = int(port)
            except (ValueError, TypeError):
                raise ImproperlyConfigured("port value must be an integer")
        else:
            host, port = None, None
            unix_socket_path = self._server

        kwargs = {
            'db': self.db,
            'password': self.password,
            'host': host,
            'port': port,
            'unix_socket_path': unix_socket_path,
        }
        connection_pool = pool.get_connection_pool(
            parser_class=self.parser_class,
            **kwargs
        )
        self.client = redis.Redis(connection_pool=connection_pool, **kwargs)

    def get_client(self, *args):
        return self.client

    @property
    def clients(self):
        return [self.client]

    ####################
    # Django cache api #
    ####################

    def add(self, key, value, timeout=None, version=None):
        """
        Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        key = self.make_key(key, version=version)
        return self._add(self.client, key, value, timeout)

    def get(self, key, default=None, version=None):
        """
        Retrieve a value from the cache.

        Returns unpickled value if key is found, the default if not.
        """
        key = self.make_key(key, version=version)
        return self._get(self.client, key, default)

    def set(self, key, value, timeout=None, version=None, client=None):
        """
        Persist a value to the cache, and set an optional expiration time.
        """
        if client is None:
            client = self.client
        key = self.make_key(key, version=version)
        return self._set(client, key, value, timeout, _add_only=False)

    def delete(self, key, version=None):
        """
        Remove a key from the cache.
        """
        key = self.make_key(key, version=version)
        return self._delete(self.client, key)

    def delete_many(self, keys, version=None):
        """
        Remove multiple keys at once.
        """
        versioned_keys = self.make_keys(keys, version=version)
        self._delete_many(self.client, versioned_keys)

    def clear(self, version=None):
        """
        Flush cache keys.

        If version is specified, all keys belonging the version's key
        namespace will be deleted.  Otherwise, all keys will be deleted.
        """
        if version is None:
            self._clear(self.client)
        else:
            self.delete_pattern('*', version=version)

    def get_many(self, keys, version=None):
        versioned_keys = self.make_keys(keys, version=version)
        return self._get_many(self.client, keys, versioned_keys=versioned_keys)

    def set_many(self, data, timeout=None, version=None):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        versioned_keys = self.make_keys(data.keys())
        if timeout is None:
            new_data = {}
            for key in versioned_keys:
                new_data[key] = data[key._original_key]
            return self._set_many(self.client, new_data)

        pipeline = self.client.pipeline()
        for key in versioned_keys:
            self._set(pipeline, key, data[key._original_key], timeout)
        pipeline.execute()

    def incr(self, key, delta=1, version=None):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        key = self.make_key(key, version=version)
        return self._incr(self.client, key, delta=delta)

    def incr_version(self, key, delta=1, version=None):
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.

        """
        if version is None:
            version = self.version

        old = self.make_key(key, version)
        new = self.make_key(key, version=version + delta)

        return self._incr_version(self.client, old, new, delta, version)

    #####################
    # Extra api methods #
    #####################

    def delete_pattern(self, pattern, version=None):
        pattern = self.make_key(pattern, version=version)
        self._delete_pattern(self.client, pattern)

    def get_or_set(self, key, func, timeout=None, version=None):
        key = self.make_key(key, version=version)
        return self._get_or_set(self.client, key, func, timeout)

    def reinsert_keys(self):
        """
        Reinsert cache entries using the current pickle protocol version.
        """
        self._reinsert_keys(self.client)
        self._print_progress(1)
        print

