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

# Dependencies come from the vcpkg manifest (vcpkg.json).
find_package(PostgreSQL REQUIRED)
find_package(Threads REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)

# The clickhouse-cpp port installs no CMake config, so the library is imported
# here together with the dependencies it was built against.
find_package(absl CONFIG REQUIRED)
find_package(cityhash CONFIG REQUIRED)
find_package(lz4 CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_path(CLICKHOUSE_CPP_INCLUDE_DIR clickhouse/client.h REQUIRED)
find_library(CLICKHOUSE_CPP_LIBRARY clickhouse-cpp-lib REQUIRED)

add_library(clickhouse-cpp::clickhouse-cpp UNKNOWN IMPORTED)
set_target_properties(clickhouse-cpp::clickhouse-cpp PROPERTIES
    IMPORTED_LOCATION "${CLICKHOUSE_CPP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${CLICKHOUSE_CPP_INCLUDE_DIR}")
target_link_libraries(clickhouse-cpp::clickhouse-cpp INTERFACE
    absl::int128 cityhash LZ4::lz4 zstd::libzstd)

add_library(recordflow_core
    src/config.cpp
    src/pgoutput/decoder.cpp
    src/runtime/log.cpp
    src/stream/printer/change_event_printer.cpp
    src/stream/replication_stream.cpp)

target_include_directories(recordflow_core PUBLIC src)
target_link_libraries(recordflow_core
    PUBLIC
        PostgreSQL::PostgreSQL
        clickhouse-cpp::clickhouse-cpp
        nlohmann_json::nlohmann_json
        tomlplusplus::tomlplusplus
        Threads::Threads)

if(MSVC)
    target_compile_options(recordflow_core PRIVATE /W4)
else()
    target_compile_options(recordflow_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(recordflow src/main.cpp)
target_link_libraries(recordflow PRIVATE recordflow_core)

option(RECORDFLOW_BUILD_TESTS "Build the unit and integration tests" ON)
if(RECORDFLOW_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
