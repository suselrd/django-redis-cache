import sys
from math import ceil
from django.core.cache.backends.base import BaseCache, InvalidCacheBackendError
from django.core.exceptions import ImproperlyConfigured
from django.utils import importlib
from redis_cache.compat import smart_bytes

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import redis
except ImportError:
    raise InvalidCacheBackendError("Redis cache backend requires the 'redis-py' library")

from redis.connection import DefaultParser

from redis_cache.utils import CacheKey


class BaseRedisCache(BaseCache):

    def __init__(self, server, params):
        """
        Connect to Redis, and set up cache backend.
        """
        self._init(server, params)

    @property
    def params(self):
        return self._params or {}

    @property
    def options(self):
        return self.params.get('OPTIONS', {})

    @property
    def db(self):
        _db = self.params.get('db', self.options.get('DB', 1))
        try:
            _db = int(_db)
        except (ValueError, TypeError):
            raise ImproperlyConfigured("db value must be an integer")
        return _db

    @property
    def password(self):
        return self.params.get('password', self.options.get('PASSWORD', None))

    @property
    def parser_class(self):
        cls = self.options.get('PARSER_CLASS', None)
        if cls is None:
            return DefaultParser
        mod_path, cls_name = cls.rsplit('.', 1)
        try:
            mod = importlib.import_module(mod_path)
            parser_class = getattr(mod, cls_name)
        except AttributeError:
            raise ImproperlyConfigured("Could not find parser class '%s'" % parser_class)
        except ImportError, e:
            raise ImproperlyConfigured("Could not find module '%s'" % e)
        return parser_class

    @property
    def pickle_version(self):
        """
        Get the pickle version from the settings and save it for future use
        """
        if self._pickle_version is None:
            _pickle_version = self.options.get('PICKLE_VERSION', -1)
            try:
                _pickle_version = int(_pickle_version)
            except (ValueError, TypeError):
                raise ImproperlyConfigured("pickle version value must be an integer")
            self._pickle_version = _pickle_version
        return self._pickle_version

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

    def __getstate__(self):
        return {'params': self._params, 'server': self._server}

    def __setstate__(self, state):
        self._init(**state)

    def serialize(self, value):
        return pickle.dumps(value, self.pickle_version)

    def deserialize(self, value):
        """
        Unpickles the given value.
        """
        value = smart_bytes(value)
        return pickle.loads(value)

    def get_value(self, original):
        try:
            value = int(original)
        except (ValueError, TypeError):
            value = self.deserialize(original)
        return value

    def prep_value(self, value):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return self.serialize(value)

    def make_key(self, key, version=None):
        if not isinstance(key, CacheKey):
            versioned_key = super(BaseRedisCache, self).make_key(key, version)
            return CacheKey(key, versioned_key)
        return key

    def make_keys(self, keys, version=None):
        return [self.make_key(key, version=version) for key in keys]

    ####################
    # Django cache api #
    ####################

    def _add(self, client, key, value, timeout):
        return self._set(client, key, value, timeout, _add_only=True)

    def add(self, key, value, timeout=None, version=None):
        """
        Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        raise NotImplementedError

    def _get(self, client, key, default=None):
        value = client.get(key)
        if value is None:
            return default
        value = self.get_value(value)
        return value

    def get(self, key, default=None, version=None):
        """
        Retrieve a value from the cache.

        Returns unpickled value if key is found, the default if not.
        """
        raise NotImplementedError

    def _set(self, client, key, value, timeout, _add_only=False):

        value = self.prep_value(value)

        if timeout is None:
            timeout = self.default_timeout

        if timeout == 0:
            if _add_only:
                return client.setnx(key, value)
            return client.set(key, value)
        elif timeout > 0:
            if _add_only:
                added = client.setnx(key, value)
                if added:
                    client.expire(key, timeout)
                return added
            return client.setex(key, value, timeout)
        else:
            return False

    def set(self, key, value, timeout=None, version=None, client=None):
        """
        Persist a value to the cache, and set an optional expiration time.
        """
        raise NotImplementedError()

    def _delete(self, client, key):
        return client.delete(key)

    def delete(self, key, version=None):
        """
        Remove a key from the cache.
        """
        raise NotImplementedError

    def _delete_many(self, client, keys):
        return client.delete(*keys)

    def delete_many(self, keys, version=None):
        """
        Remove multiple keys at once.
        """
        raise NotImplementedError

    def _clear(self, client):
        return client.flushdb()

    def clear(self, version=None):
        """
        Flush cache keys.

        If version is specified, all keys belonging the version's key
        namespace will be deleted.  Otherwise, all keys will be deleted.
        """
        raise NotImplementedError

    def _get_many(self, client, original_keys, versioned_keys):
        """
        Retrieve many keys.
        """
        recovered_data = {}
        map_keys = dict(zip(versioned_keys, original_keys))

        results = client.mget(versioned_keys)

        for key, value in zip(versioned_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.get_value(value)

        return recovered_data

    def get_many(self, keys, version=None):
        raise NotImplementedError

    def _set_many(self, client, data):
        new_data = {}
        for key, value in data.items():
            new_data[key] = self.prep_value(value)

        return client.mset(new_data)

    def set_many(self, data, timeout=None, version=None):
        """
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        raise NotImplementedError

    def _incr(self, client, key, delta=1):
        exists = client.exists(key)
        if not exists:
            raise ValueError("Key '%s' not found" % key)
        try:
            value = client.incr(key, delta)
        except redis.ResponseError:
            value = self._get(client, key) + delta
            self._set(client, key, value, timeout=None)
        return value

    def incr(self, key, delta=1, version=None):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        raise NotImplementedError

    def _incr_version(self, client, old, new, delta, version):
        try:
            client.rename(old, new)
        except redis.ResponseError:
            raise ValueError("Key '%s' not found" % old._original_key)

        return version + delta

    def incr_version(self, key, delta=1, version=None):
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.

        """
        raise NotImplementedError

    #####################
    # Extra api methods #
    #####################

    def _delete_pattern(self, client, pattern):
        keys = client.keys(pattern)
        if len(keys):
            client.delete(*keys)

    def delete_pattern(self, pattern, version=None):
        raise NotImplementedError

    def _get_or_set(self, client, key, func, timeout=None):
        if not callable(func):
            raise Exception("func must be a callable")

        dogpile_lock_key = "_lock" + key._versioned_key
        dogpile_lock = client.get(dogpile_lock_key)

        if dogpile_lock is None:
            self._set(client, dogpile_lock_key, 0, None)
            value = func()
            self._set(client, key, self.prep_value(value), None)
            self._set(client, dogpile_lock_key, 0, timeout)
        else:
            value = self._get(client, key)

        return value

    def get_or_set(self, key, func, timeout=None, version=None):
        raise NotImplementedError

    def _print_progress(self, progress):
        """
        Helper function to print out the progress of the reinsertion.
        """
        sys.stdout.flush()
        progress = int(ceil(progress * 80))
        msg = "Reinserting keys: |%s|\r" % (progress * "=" + (80 - progress) * " ")
        sys.stdout.write(msg)

    def _reinsert_keys(self, client):
        keys = client.keys('*')
        for i, key in enumerate(keys):
            timeout = client.ttl(key)
            value = self.deserialize(client.get(key))

            if timeout is None:
                client.set(key, self.prep_value(value))

            progress = float(i) / len(keys)
            self._print_progress(progress)

    def reinsert_keys(self):
        """
        Reinsert cache entries using the current pickle protocol version.
        """
        raise NotImplementedError
