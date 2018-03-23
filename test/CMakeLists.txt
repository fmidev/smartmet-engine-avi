get_filename_component(ENGINE_CMAKE_DIR ${CMAKE_BINARY_DIR} DIRECTORY)
get_filename_component(ENGINE_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR} DIRECTORY)


find_package(Boost QUIET COMPONENTS date_time thread filesystem iostreams unit_test_framework REQUIRED)
find_package (Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BZip2 QUIET)
if(NOT BZIP2_FOUND)
    find_path(BZIP2_INCLUDE_DIR bzlib.h ${CMAKE_INSTALL_PREFIX} PATH_SUFFIXES include)
    find_library(BZIP2_LIBRARIES NAMES bz2 libbz2 bzip2 libbzip2 ${CMAKE_INSTALL_PREFIX} PATH_SUFFIXES lib a so)
    message(STATUS "BZIP2_INCLUDE_DIR: ${BZIP2_INCLUDE_DIR}")
    message(STATUS "BZIP2_LIBRARIES: ${BZIP2_LIBRARIES}")
endif()
find_library(Spine_LIBRARY smartmet-spine)
find_path(Spine_INCLUDE_DIR ConfigBase.h ${CMAKE_INSTALL_PREFIX} /usr/include/smartmet/spine)
if (NOT Spine_INCLUDE_DIR)
  message(FATAL_ERROR "SmartMet Spine include director not found")
endif()
find_library(MacGyver_LIBRARY smartmet-macgyver)
find_path(MacGyver_INCLUDE_DIR TypeName.h ${CMAKE_INSTALL_PREFIX} /usr/include/smartmet/macgyver)
if (NOT MacGyver_INCLUDE_DIR)
  message(FATAL_ERROR "SmartMet MacGyver include directory not found")
endif()
find_library(Avi_LIBRARY avi ${Avi_SOURCE_DIR})
if (NOT Avi_LIBRARY)
  message(FATAL_ERROR "SmartMet Avi Engine library not found")
endif()
find_library(PQXX_LIBRARY pqxx)
if (NOT PQXX_LIBRARY)
  message(FATAL_ERROR "PQXX library not found")
endif()


get_filename_component(SmartMet_INCLUDE_DIR ${Spine_INCLUDE_DIR} DIRECTORY)
file(GLOB UNIT_TEST_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.cpp)
include_directories(${CMAKE_BINARY_DIR}/include ${SmartMet_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})
add_compile_options(-std=c++11 -Wall -W -Wno-unused-parameter)
set(CMAKE_CXX_FLAGS "-DUNIX")


# Copy config files
file(GLOB SAMPLE_CONFIC_FILES ${Avi_SOURCE_DIR}/test/cnf/*.conf)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/cnf)
foreach(configFileName ${SAMPLE_CONFIC_FILES})
  file(COPY ${configFileName} DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/cnf)
endforeach(configFileName)

foreach(testSrc ${UNIT_TEST_SRCS})
  get_filename_component(testName ${testSrc} NAME_WE)
  add_executable(${testName} ${testSrc})
  target_link_libraries(
    ${testName}
    ${CMAKE_STANDARD_LIBRARIES}
    ${AVI_LIBRARIES}
    ${Boost_LIBRARIES}
    ${SmartTools_LIBRARIES}
    ${BZIP2_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${Spine_LIBRARY}
    ${MacGyver_LIBRARY}
    ${Avi_LIBRARY}
    ${PQXX_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})

#  add_dependencies(${testName} ${PROJECT_NAME})

  # Move testing binaries into a testBin directory
  set_target_properties(${testName} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY  ${CMAKE_CURRENT_BINARY_DIR})
  add_test(NAME ${testName}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${testName})
endforeach(testSrc)
