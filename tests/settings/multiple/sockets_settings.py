from tests.settings.base_settings import *
from os.path import join, dirname


CACHES = {
    'default': {
        'BACKEND': 'redis_cache.ShardedRedisCache',
        'LOCATION': join(dirname(__file__), 'redis.sock'),
        'OPTIONS': {
            'DB': 15,
            'PASSWORD': 'yadayada',
            'PARSER_CLASS': 'redis.connection.HiredisParser',
            'PICKLE_VERSION': 2,
        },
    },
}

