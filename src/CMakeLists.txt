set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")


add_executable(ximulator_main
        main.cc)
target_link_libraries(ximulator_main
        ximulator)

add_library(ximulator STATIC
        events.cc)
target_link_libraries(ximulator
        scheduler
        traffic_gen)

add_library(scheduler STATIC
        scheduler.cc
        schedulerAalo.cc
        schedulerVarys.cc)
target_link_libraries(scheduler
        coflow
        usage_monitor
        ximulator)

add_library(traffic_gen STATIC
        traffic_generator.cc
        traffic_analyzer.cc
        )
target_link_libraries(traffic_gen
        coflow
        usage_monitor
        ximulator)


add_library(global STATIC
        global.cc)
target_link_libraries(global
        util)

add_library(util STATIC
        util.cc)

add_library(coflow STATIC
        coflow.cc)
target_link_libraries(coflow
        global
        util)

if (UNIX AND NOT APPLE)
    # include directory for remote linux nodes
    include_directories(
            /usr/include/mysql
            /usr/include/mysql++)
elseif(APPLE)
    # library under local macbook
    include_directories(
            /usr/local/include/mysql
            /usr/local/include/mysql++)
endif ()

add_library(db_logger
        db_logger.cc)
target_link_libraries(db_logger
        coflow
        util)
if (UNIX AND NOT APPLE)
    # library under remote linux nodes
    target_link_libraries(db_logger
            /usr/lib/libmysqlpp.so)
elseif(APPLE)
    # library under local macbook
    target_link_libraries(db_logger
            /usr/local/lib/libmysqlpp.dylib)
endif ()

add_library(usage_monitor
        usage_monitor.cc)
target_link_libraries(usage_monitor
        db_logger
        global)