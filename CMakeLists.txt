cmake_minimum_required(VERSION 3.20)

project(recordflow
    VERSION 0.1.0
    DESCRIPTION "DBLog-style change data capture engine for PostgreSQL"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "" FORCE)
endif()

find_package(PostgreSQL REQUIRED)
find_package(Threads REQUIRED)

add_library(recordflow_core
    src/config.cpp
    src/stream/parser/pgoutput_parser.cpp
    src/stream/printer/change_event_printer.cpp
    src/stream/replication_stream.cpp)

target_include_directories(recordflow_core PUBLIC src)
target_link_libraries(recordflow_core
    PUBLIC
        PostgreSQL::PostgreSQL
        Threads::Threads)

if(WIN32)
    target_link_libraries(recordflow_core PUBLIC ws2_32 crypt32 secur32)
endif()

if(MSVC)
    target_compile_options(recordflow_core PRIVATE /W4)
else()
    target_compile_options(recordflow_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(recordflow src/main.cpp)
target_link_libraries(recordflow PRIVATE recordflow_core)
