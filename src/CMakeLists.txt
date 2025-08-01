##=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https:#www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

cmake_minimum_required(VERSION 3.20)

macro(LLFS_DefineLibrary TARGET_NAME TARGET_SRCDIR)

  set(TARGET_DEPS ${ARGN})
  
  file(GLOB ${TARGET_NAME}_LibSources
    ./${TARGET_SRCDIR}/*.cpp
    ./${TARGET_SRCDIR}/*/*.cpp
    ./${TARGET_SRCDIR}/*/*/*.cpp
    ./${TARGET_SRCDIR}/*/*/*/*.cpp
  )

  file(GLOB ${TARGET_NAME}_TestSources
    ./${TARGET_SRCDIR}/*_test.cpp
    ./${TARGET_SRCDIR}/*/*_test.cpp
    ./${TARGET_SRCDIR}/*/*/*_test.cpp
    ./${TARGET_SRCDIR}/*/*/*/*_test.cpp
    ./${TARGET_SRCDIR}/*.test.cpp
    ./${TARGET_SRCDIR}/*/*.test.cpp
    ./${TARGET_SRCDIR}/*/*/*.test.cpp
    ./${TARGET_SRCDIR}/*/*/*/*.test.cpp
    )
  
  foreach (_file "FORCE_LIST_NOT_EMPTY;${${TARGET_NAME}_TestSources}")
    list(REMOVE_ITEM ${TARGET_NAME}_LibSources ${_file})
  endforeach ()

  # Define the library target
  #
  add_library(${TARGET_NAME} ${${TARGET_NAME}_LibSources})
  target_link_libraries(${TARGET_NAME} ${TARGET_DEPS})

  # Define unit test target
  #
  add_executable(${TARGET_NAME}_Test ${${TARGET_NAME}_TestSources} ./common/test_environment.cpp)
  target_link_libraries(
    ${TARGET_NAME}_Test
    ${TARGET_NAME}
    ${TARGET_DEPS}
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
    GTest::gmock_main
    libfuse::libfuse
    OpenSSL::Crypto
    Boost::context
    Boost::stacktrace_backtrace
    libbacktrace::libbacktrace
    xxHash::xxhash
    dl
    stdc++fs)

  add_test(NAME ${TARGET_NAME}_Test
           WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
           COMMAND ${TARGET_NAME}_Test)

  add_custom_target(test_${TARGET_NAME}
                    build/${CMAKE_BUILD_TYPE}/bin/${TARGET_NAME}_Test
                    DEPENDS ${TARGET_NAME}_Test)

  add_dependencies(RunAllLibTests test_${TARGET_NAME})

  # Default packaging instructions for the library.
  #
  install(TARGETS ${TARGET_NAME} DESTINATION "."
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    )

  file(GLOB_RECURSE LLFS_HEADERS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ./${TARGET_SRCDIR}/*.hpp)
  
  foreach(_header IN LISTS LLFS_HEADERS)
    get_filename_component(_destination "${_header}" PATH)
    install(FILES "${_header}" DESTINATION include/${_destination})
  endforeach()

endmacro()
# LLFS_DefineLibrary
#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -


#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
# Library definitions.
#

LLFS_DefineLibrary(llfs ./llfs
  batteries::batteries
  liburing::liburing
  OpenSSL::Crypto
  xxHash::xxhash
  )

#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

file(GLOB llfs_cli_Sources
  ./llfs_cli/*.cpp
  ./llfs_cli/*/*.cpp
  ./llfs_cli/*/*/*.cpp
  )

add_executable(llfs_cli ${llfs_cli_Sources})

target_link_libraries(llfs_cli
                      llfs
                      batteries::batteries
                      Boost::context
                      Boost::stacktrace_backtrace
                      libbacktrace::libbacktrace
                      CLI11::CLI11
                      dl
                      stdc++fs)

set_target_properties(llfs_cli PROPERTIES OUTPUT_NAME "llfs")

# Default packaging instructions for the library.
#
install(TARGETS llfs_cli DESTINATION "."
  RUNTIME DESTINATION bin
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  )

file(GLOB_RECURSE LLFS_CLI_HEADERS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ./llfs_cli/*.hpp)

foreach(_header IN LISTS LLFS_CLI_HEADERS)
    get_filename_component(_destination "${_header}" PATH)
    install(FILES "${_header}" DESTINATION include/${_destination})
endforeach()

#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

file(GLOB llfs_fuse_Sources
  ./llfs_fuse/*.cpp
  ./llfs_fuse/*/*.cpp
  ./llfs_fuse/*/*/*.cpp
  )

add_executable(llfs_fuse ${llfs_fuse_Sources})

target_link_libraries(llfs_fuse
  llfs
  libfuse::libfuse
  batteries::batteries
  Boost::context
  Boost::stacktrace_backtrace
  libbacktrace::libbacktrace
  OpenSSL::Crypto
  CLI11::CLI11
  dl
  stdc++fs)