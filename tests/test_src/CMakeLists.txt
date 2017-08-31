set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(gtest_SOURCE_DIR
        ../test_lib/googletest)
set(ximulator_SOURCE_DIR
        ../..)

include_directories(
        ${gtest_SOURCE_DIR}/googlemock/include
        ${gtest_SOURCE_DIR}/googletest/include
        ${gtest_SOURCE_DIR}
        ${ximulator_SOURCE_DIR})

add_executable(ximulator_test
        ximulator_test.cc)
target_link_libraries(ximulator_test
        gtest
        gtest_main
        ximulator)

add_executable(traffic_generator_test
        traffic_generator_test.cc)
target_link_libraries(traffic_generator_test
        gtest
        gtest_main
        ximulator)

add_executable(usage_monitor_test
        usage_monitor_test.cc)
target_link_libraries(usage_monitor_test
        gtest
        gtest_main
        usage_monitor
        ximulator)