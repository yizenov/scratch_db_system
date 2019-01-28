cmake_minimum_required(VERSION 3.12)
project(db_systems)

set(CMAKE_CXX_STANDARD 14)

add_executable(db_systems Config.h Schema.h Schema.cc Catalog.h Catalog.cc main.cc)