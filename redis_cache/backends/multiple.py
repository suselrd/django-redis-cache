import sys
from collections import defaultdict
from math import ceil
from django.core.cache.backends.base import BaseCache, InvalidCacheBackendError
from django.core.exceptions import ImproperlyConfigured
from django.utils import importlib
from django.utils.encoding import smart_unicode, smart_str
from django.utils.datastructures import SortedDict

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import redis
except ImportError:
    raise InvalidCacheBackendError("Redis cache backend requires the 'redis-py' library")

from redis.connection import DefaultParser

from redis_cache.backends.base import BaseRedisCache
from redis_cache.sharder import CacheSharder
from redis_cache.connection import pool
from redis_cache.utils import CacheKey


class ShardedRedisCache(BaseRedisCache):

    def __init__(self, server, params):
        """
        Connect to Redis, and set up cache backend.
        """
        self._init(server, params)

    def _init(self, server, params):
        super(BaseRedisCache, self).__init__(params)
        self._params = params
        self._server = server
        self._pickle_version = None
        self.__master_client = None
        self.clients = []
        self.sharder = CacheSharder()

        if not isinstance(server, (list, tuple)):
            servers = [server]
        else:
            servers = server

        for server in servers:
            unix_socket_path = None
            if ':' in server:
                host, port = server.rsplit(':', 1)
                try:
                    port = int(port)
                except (ValueError, TypeError):
                    raise ImproperlyConfigured("port value must be an integer")
            else:
                host, port = None, None
                unix_socket_path = server

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
            client = redis.Redis(
                connection_pool=connection_pool,
                **kwargs
            )
            self.clients.append(client)
            self.sharder.add(client, "%s:%s" % (host, port))

    @property
    def master_client(self):
        """
        Get the write server:port of the master cache
        """
        if not hasattr(self, '_master_client') and self.__master_client is None:
            cache = self.options.get('MASTER_CACHE', None)
            if cache is None:
                self._master_client = None
            else:
                self._master_client = None
                try:
                    host, port = cache.split(":")
                except ValueError:
                    raise ImproperlyConfigured("MASTER_CACHE must be in the form <host>:<port>")
                for client in self.clients:
                    connection_kwargs = client.connection_pool.connection_kwargs
                    if connection_kwargs['host'] == host and connection_kwargs['port'] == int(port):
                        self._master_client = client
                        break
                if self._master_client is None:
                    raise ImproperlyConfigured("%s is not in the list of available redis-server instances." % cache)
        return self._master_client

    def get_client(self, key, for_write=False):
        if for_write and self.master_client is not None:
            return self.master_client
        return self.sharder.get_client(key)

    def shard(self, keys, for_write=False, version=None):
        """
        Returns a dict of keys that belong to a cache's keyspace.
        """
        clients = defaultdict(list)
        for key in keys:
            clients[self.get_client(key, for_write)].append(self.make_key(key, version))
        return clients

    ####################
    # Django cache api #
    ####################

    def add(self, key, value, timeout=None, version=None):
        """
        Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        client = self.get_client(key)
        key = self.make_key(key, version=version)
        return self._add(client, key, value, timeout)

    def get(self, key, default=None, version=None):
        """
        Retrieve a value from the cache.

        Returns unpickled value if key is found, the default if not.
        """
        client = self.get_client(key)
        key = self.make_key(key, version=version)

        return self._get(client, key, default)

    def set(self, key, value, timeout=None, version=None, client=None):
        """
        Persist a value to the cache, and set an optional expiration time.
        """
        if client is None:
            client = self.get_client(key, for_write=True)
        key = self.make_key(key, version=version)
        return self._set(client, key, value, timeout, _add_only=False)

    def delete(self, key, version=None):
        """
        Remove a key from the cache.
        """
        client = self.get_client(key, for_write=True)
        key = self.make_key(key, version=version)
        return self._delete(client, key)

    def delete_many(self, keys, version=None):
        """
        Remove multiple keys at once.
        """
        clients = self.shard(keys, for_write=True, version=version)
        for client, keys in clients.items():
            self._delete_many(client, keys)

    def clear(self, version=None):
        """
        Flush cache keys.

        If version is specified, all keys belonging the version's key
        namespace will be deleted.  Otherwise, all keys will be deleted.
        """
        if version is None:
            if self.master_client is None:
                for client in self.clients:
                    self._clear(client)
            else:
                self._clear(self.master_client)
        else:
            self.delete_pattern('*', version=version)

    def get_many(self, keys, version=None):
        data = {}
        clients = self.shard(keys, version=version)
        for client, versioned_keys in clients.items():
            original_keys = [key._original_key for key in versioned_keys]
            data.update(self._get_many(client, original_keys, versioned_keys=versioned_keys))
        return data

    def set_many(self, data, timeout=None, version=None):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        clients = self.shard(data.keys(), for_write=True, version=version)

        if timeout is None:
            for client, keys in clients.iteritems():
                subset = {}
                for key in keys:
                    subset[key] = data[key._original_key]
                self._set_many(client, subset)
            return

        for client, keys in clients.iteritems():
            pipeline = client.pipeline()
            for key in keys:
                self._set(pipeline, key, data[key._original_key], timeout)
            pipeline.execute()

    def incr(self, key, delta=1, version=None):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        client = self.get_client(key, for_write=True)
        key = self.make_key(key, version=version)
        return self._incr(client, key, delta=delta)

    def incr_version(self, key, delta=1, version=None):
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.

        """
        if version is None:
            version = self.version

        client = self.get_client(key, for_write=True)
        old = self.make_key(key, version=version)
        new = self.make_key(key, version=version + delta)

        return self._incr_version(client, old, new, delta, version)

    #####################
    # Extra api methods #
    #####################

    def delete_pattern(self, pattern, version=None):
        pattern = self.make_key(pattern, version=version)
        if self.master_client is None:
            for client in self.clients:
                self._delete_pattern(client, pattern)
        else:
            keys = self.master_client.keys(pattern)
            self._delete_pattern(self.master_client, pattern)

    def get_or_set(self, key, func, timeout=None, version=None):
        key = self.make_key(key, version=version)
        client = self.get_client(key, for_write=True)
        return self._get_or_set(client, key, func, timeout)

    def reinsert_keys(self):
        """
        Reinsert cache entries using the current pickle protocol version.
        """

        for i, client in enumerate(self.clients):
            self._reinsert_keys(client)
        self._print_progress(1)
        print
