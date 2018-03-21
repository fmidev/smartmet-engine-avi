cmake_minimum_required(VERSION 3.10.1)
project(avi)


STRING(COMPARE EQUAL "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}" isSourceDir)
    IF(isSourceDir)
        MESSAGE(FATAL_ERROR "building in source is not supported. Create a build folder somewhere else (e.g. ./build).")
ENDIF()

option(MAKE_TESTS "Make the tests" ON)

include_directories(avi)

if (MAKE_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()
