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

# librdkafka is the C client for the Kafka protocol and carries decoded change
# events to the output topic. How it advertises itself to CMake depends on how
# the distributor built it, so try all three routes and normalise whatever is
# found behind a single target.
#
#   Homebrew (macOS): brew install librdkafka
#   MSYS2 UCRT64:     pacman -S mingw-w64-ucrt-x86_64-librdkafka
add_library(recordflow_rdkafka INTERFACE)

find_package(RdKafka CONFIG QUIET)
if(TARGET RdKafka::rdkafka)
    # librdkafka's own CMake build installs RdKafkaConfig.cmake.
    target_link_libraries(recordflow_rdkafka INTERFACE RdKafka::rdkafka)
    set(RECORDFLOW_RDKAFKA_ORIGIN "CMake package config")
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(RDKAFKA QUIET IMPORTED_TARGET rdkafka)
    endif()

    if(TARGET PkgConfig::RDKAFKA)
        # Builds that went through librdkafka's mklove ./configure ship
        # rdkafka.pc but no CMake config. This is the Homebrew case.
        target_link_libraries(recordflow_rdkafka INTERFACE PkgConfig::RDKAFKA)
        set(RECORDFLOW_RDKAFKA_ORIGIN "pkg-config (rdkafka.pc)")
    else()
        find_path(RDKAFKA_INCLUDE_DIR NAMES librdkafka/rdkafka.h)
        find_library(RDKAFKA_LIBRARY NAMES rdkafka)
        if(NOT RDKAFKA_INCLUDE_DIR OR NOT RDKAFKA_LIBRARY)
            message(FATAL_ERROR
                "librdkafka not found. Install it (macOS: `brew install "
                "librdkafka`; MSYS2 UCRT64: `pacman -S "
                "mingw-w64-ucrt-x86_64-librdkafka`), or point CMAKE_PREFIX_PATH "
                "at an existing install.")
        endif()
        target_include_directories(recordflow_rdkafka INTERFACE ${RDKAFKA_INCLUDE_DIR})
        target_link_libraries(recordflow_rdkafka INTERFACE ${RDKAFKA_LIBRARY})
        set(RECORDFLOW_RDKAFKA_ORIGIN "manual search (${RDKAFKA_LIBRARY})")
    endif()
endif()

message(STATUS "librdkafka located via: ${RECORDFLOW_RDKAFKA_ORIGIN}")

# --- Library: the reusable CDC core -----------------------------------------
add_library(recordflow_core
    src/config.cpp
        src/stream/parser/pgoutput_parser.cpp
        src/stream/replication_stream.cpp)

target_include_directories(recordflow_core PUBLIC src)
target_link_libraries(recordflow_core
    PUBLIC
        PostgreSQL::PostgreSQL
        recordflow_rdkafka
        Threads::Threads)

# Sockets need Winsock on Windows; a statically linked librdkafka also pulls in
# the SChannel and CryptoAPI surfaces for TLS and SASL. Both are part of the
# MinGW runtime, so naming them is harmless when librdkafka is a DLL.
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
