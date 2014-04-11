from setuptools import setup

setup(
    name = "django-redis-cache",
    url = "http://github.com/suselrd/django-redis-cache/",
    author = "Susel Ruiz Duran",
    author_email = "suselrd@gmail.com",
    version = "0.11.2",  # This is a fork of the 0.11.1 version of the django-redis-cache project
    packages = ["redis_cache"],
    description = "Redis Cache Backend for Django",
    install_requires=['redis>=2.4.5',],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
        "Environment :: Web Environment",
        "Framework :: Django",
    ],
)
