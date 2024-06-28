cmake_minimum_required(VERSION 3.0.0)
project(redis_clone VERSION 0.1.0 LANGUAGES C CXX)

add_executable(redis_clone server.cpp avl.cpp hashtable.cpp heap.cpp thread_pool.cpp zset.cpp)

