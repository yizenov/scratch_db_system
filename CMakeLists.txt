cmake_minimum_required(VERSION 3.12)
project(db_systems)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES Swap.h
        Swapify.h Swapify.cc
        ComplexSwapify.h ComplexSwapify.cc
        Keyify.h Keyify.cc
        TwoWayList.h TwoWayList.cc
        InefficientMap.h InefficientMap.cc
        EfficientMap.h EfficientMap.cc
        Config.h
        Schema.h Schema.cc
        Catalog.h Catalog.cc
        ParseTree.h
        QueryCompiler.h QueryCompiler.cc
        QueryOptimizer.h QueryOptimizer.cc
        Record.h Record.cc
        RelOp.h RelOp.cc
        Function.h Function.cc
        File.h File.cc
        DBFile.h DBFile.cc
        Comparison.h Comparison.cc
        main.cc)

add_executable(db_systems ${SOURCE_FILES})

target_link_libraries(db_systems LINK_PRIVATE  ${CMAKE_CURRENT_SOURCE_DIR}/QueryLexer.o  ${CMAKE_CURRENT_SOURCE_DIR}/QueryParser.o LINK_PUBLIC sqlite3)