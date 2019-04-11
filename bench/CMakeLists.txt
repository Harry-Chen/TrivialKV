cmake_minimum_required(VERSION 2.8)

set(EXE bench)

# disable all warnings for benchmark
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

add_executable(${EXE} bench.cc bench_util.h zipf.h)
target_link_libraries(${EXE} engine ${CMAKE_THREAD_LIBS_INIT})
