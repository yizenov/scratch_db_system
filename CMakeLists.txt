cmake_minimum_required(VERSION 3.12)
project(db_systems)

set(CMAKE_CXX_STANDARD 14)

add_executable(db_systems
        Swap.h
        Swapify.h Swapify.cc
        Keyify.h Keyify.cc
        TwoWayList.h TwoWayList.cc
        InefficientMap.h InefficientMap.cc
        EfficientMap.h EfficientMap.cc
        Config.h
        Schema.h Schema.cc
        Catalog.h Catalog.cc
        main.cc)
target_link_libraries(db_systems LINK_PUBLIC sqlite3)