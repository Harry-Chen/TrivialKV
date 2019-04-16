cmake_minimum_required(VERSION 2.8)
project(TrivialKV LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -march=native -mtune=native -Wall -g -D_GNU_SOURCE -lrt")

find_package(Threads)

include_directories(".")

add_subdirectory(engine_race)
add_subdirectory(test)
add_subdirectory(bench)
