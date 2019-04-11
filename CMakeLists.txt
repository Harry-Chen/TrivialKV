cmake_minimum_required(VERSION 2.8)
project(TrivialKV LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -g -D_GNU_SOURCE")

include_directories(".")

add_subdirectory(engine_race)
add_subdirectory(test)