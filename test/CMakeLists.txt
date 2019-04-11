cmake_minimum_required(VERSION 2.8)

find_package (Threads)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -lrt")

foreach(TEST single_thread_test multi_thread_test crash_test)
    add_executable(${TEST} ${TEST}.cc)
    target_link_libraries(${TEST} engine ${CMAKE_THREAD_LIBS_INIT})
endforeach(TEST)
    