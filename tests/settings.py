DEBUG = True

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
    }
}

INSTALLED_APPS = [
    'tests.testapp',
]

CACHES = {
    'default': {
        'BACKEND': 'redis_cache.RedisCache',
        'LOCATION': '127.0.0.1:6379',
        'OPTIONS': {  # optional
            'CONNECTION_POOL_CLASS_KWARGS': {
                'max_connections': 2
            }
        },
    },
}

ROOT_URLCONF = 'tests.urls'

SECRET_KEY = 'blabla'
