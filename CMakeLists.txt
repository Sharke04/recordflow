cmake_minimum_required(VERSION 3.20)

project(recordflow
    VERSION 0.1.0
    DESCRIPTION "DBLog-style change data capture engine for PostgreSQL"
    LANGUAGES CXX)

# --- Global settings --------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Export compile_commands.json so clangd / editors get accurate IntelliSense.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "" FORCE)
endif()

# --- Dependencies -----------------------------------------------------------
# libpq is the C client library for PostgreSQL. CMake ships a FindPostgreSQL
# module that locates libpq-fe.h and the pq import library. In MSYS2 UCRT64 it
# is provided by the `mingw-w64-ucrt-x86_64-postgresql` package; on macOS by
# Homebrew's keg-only `libpq` (the presets point PostgreSQL_ROOT at it).
find_package(PostgreSQL REQUIRED)

# The output buffer of section 3.1 is drained by a thread separate from the
# replication read loop, so the standard threading library is a hard
# requirement rather than something libpq happens to drag in.
find_package(Threads REQUIRED)

# --- Library: the reusable CDC core -----------------------------------------
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

# Sockets need Winsock on Windows; a statically linked libpq also pulls in the
# SSPI and CryptoAPI surfaces for authentication and TLS. All are part of the
# MinGW runtime, so naming them is harmless when libpq is a DLL.
if(WIN32)
    target_link_libraries(recordflow_core PUBLIC ws2_32 crypt32 secur32)
endif()

if(MSVC)
    target_compile_options(recordflow_core PRIVATE /W4)
else()
    target_compile_options(recordflow_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

# --- Executable -------------------------------------------------------------
add_executable(recordflow src/main.cpp)
target_link_libraries(recordflow PRIVATE recordflow_core)
