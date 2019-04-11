cmake_minimum_required(VERSION 3.12)
project(db_systems)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES Swap.h Config.h ParseTree.h
        Swapify.h Swapify.cc
        ComplexSwapify.h ComplexSwapify.cc
        TwoWayList.h TwoWayList.cc
        Keyify.h Keyify.cc
        Schema.h Schema.cc
        Catalog.h Catalog.cc
        Record.h Record.cc
        ComplexKeyify.h ComplexKeyify.cc
        InefficientMap.h InefficientMap.cc
        EfficientMap.h EfficientMap.cc
        File.h File.cc
        DBFile.h DBFile.cc
        Comparison.h Comparison.cc
        Function.h Function.cc
        RelOp.h RelOp.cc
        QueryOptimizer.h QueryOptimizer.cc
        QueryCompiler.h QueryCompiler.cc
        main.cc)

add_executable(db_systems ${SOURCE_FILES})

target_link_libraries(db_systems LINK_PRIVATE  ${CMAKE_CURRENT_SOURCE_DIR}/QueryLexer.o  ${CMAKE_CURRENT_SOURCE_DIR}/QueryParser.o LINK_PUBLIC sqlite3)