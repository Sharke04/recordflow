find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

# Unit tests: pure logic, no external services.
file(GLOB_RECURSE RECORDFLOW_UNIT_TEST_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/unit/*.cpp)

if(RECORDFLOW_UNIT_TEST_SOURCES)
    add_executable(recordflow_unit_tests ${RECORDFLOW_UNIT_TEST_SOURCES})
    target_link_libraries(recordflow_unit_tests PRIVATE recordflow_core GTest::gtest_main)
    target_compile_definitions(recordflow_unit_tests PRIVATE
        RECORDFLOW_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/unit/data")
    gtest_discover_tests(recordflow_unit_tests PROPERTIES LABELS unit)
endif()

# Integration tests: need RECORDFLOW_IT=1 and a running docker compose,
# otherwise every test is skipped.
file(GLOB_RECURSE RECORDFLOW_IT_SUPPORT_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/integration/support/*.cpp)
file(GLOB RECORDFLOW_IT_TEST_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/integration/*_test.cpp)

if(RECORDFLOW_IT_SUPPORT_SOURCES)
    add_library(recordflow_it_support STATIC ${RECORDFLOW_IT_SUPPORT_SOURCES})
    target_include_directories(recordflow_it_support PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/integration)
    target_compile_definitions(recordflow_it_support PRIVATE
        RECORDFLOW_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
    target_link_libraries(recordflow_it_support PUBLIC recordflow_core GTest::gtest)
endif()

if(RECORDFLOW_IT_TEST_SOURCES)
    add_executable(recordflow_integration_tests ${RECORDFLOW_IT_TEST_SOURCES})
    target_link_libraries(recordflow_integration_tests PRIVATE recordflow_it_support GTest::gtest_main)
    gtest_discover_tests(recordflow_integration_tests PROPERTIES LABELS integration)
endif()
