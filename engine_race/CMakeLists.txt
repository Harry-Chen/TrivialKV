cmake_minimum_required(VERSION 2.8)

add_library(engine STATIC
        engine_race.cc
        engine_race.h
        database.cc
        database.h
        utils.hpp
        index_tree.cc
        index_tree.h
        )
