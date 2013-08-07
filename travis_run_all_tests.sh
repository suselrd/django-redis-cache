#!/bin/bash

./run_single_tests.py --settings=tests.settings.single.hiredis_settings -s ../redis/src/redis-server --redis-version=$REDIS_VERSION
./run_single_tests.py --settings=tests.settings.single.python_parser -s ../redis/src/redis-server --redis-version=$REDIS_VERSION
./run_single_tests.py --settings=tests.settings.single.sockets_settings -s ../redis/src/redis-server --redis-version=$REDIS_VERSION

./run_multi_tests.py --settings=tests.settings.multiple.hiredis_settings -s ../redis/src/redis-server --redis-version=$REDIS_VERSION
./run_multi_tests.py --settings=tests.settings.multiple.python_parser -s ../redis/src/redis-server --redis-version=$REDIS_VERSION
./run_multi_tests.py --settings=tests.settings.multiple.sockets_settings -s ../redis/src/redis-server --redis-version=$REDIS_VERSION
