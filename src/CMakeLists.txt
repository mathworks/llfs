##=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https:#www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

cmake_minimum_required(VERSION 3.16)
project(LLFS CXX)

add_definitions("-fno-diagnostics-color")

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -               
# Set the language version.
#
add_compile_options(-std=c++17)

# Set options needed for stack tracing.
#
add_compile_options(-D_GNU_SOURCE -fno-omit-frame-pointer)

# Set warning/error level for the compilation.
#
add_compile_options(-Wall -Wextra -Werror -Werror=switch-enum -Wswitch-enum)
add_compile_options(-Wsuggest-override -Woverloaded-virtual)

# Configuration macros for Boost.
#
add_compile_definitions(BOOST_ERROR_CODE_HEADER_ONLY)
if(NOT APPLE)
    add_compile_definitions(BOOST_STACKTRACE_USE_BACKTRACE)
endif()

# liburing/2.1 Conan package conflicts with linux system headers, but
# defining this macro resolves the conflict.
#
add_compile_definitions(_BITS_UIO_EXT_H=1)

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -               

set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer")

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -march=native -mtune=native ") # -pg
set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -march=native -mtune=native") # -pg

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

include_directories(.)

if(APPLE)
  set(BACKTRACE_DEPENDENCY "")
else()
  set(BACKTRACE_DEPENDENCY "backtrace")
endif()

enable_testing()

add_custom_target(RunAllLibTests)

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -               
# Define a submodule of LLFS.
#
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
  
  list(REMOVE_ITEM ${TARGET_NAME}_LibSources "FORCE_LIST_NOT_EMPTY;${${TARGET_NAME}_TestSources}")

  # Define the library target
  #
  add_library(${TARGET_NAME} ${${TARGET_NAME}_LibSources})
  target_link_libraries(${TARGET_NAME} ${TARGET_DEPS})

  # Define unit test target
  #
  add_executable(${TARGET_NAME}_Test ${${TARGET_NAME}_TestSources} ./common/test_environment.cpp)
  target_link_libraries(${TARGET_NAME}_Test ${TARGET_NAME}
                        ${TARGET_DEPS}    
                        ${CONAN_LIBS_GTEST}
                        ${CONAN_LIBS}
                        backtrace
                        boost_context
                        boost_stacktrace_backtrace
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

endmacro()
# LLFS_DefineLibrary
#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -               


#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
# Submodule definitions.
#

LLFS_DefineLibrary(llfs ./llfs)

#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
                   
file(GLOB llfs_cli_Sources
  ./llfs_cli/*.cpp
  ./llfs_cli/*/*.cpp
  ./llfs_cli/*/*/*.cpp
  )
  
add_executable(llfs_cli ${llfs_cli_Sources})

target_link_libraries(llfs_cli
                      llfs
                      ${CONAN_LIBS}
                      backtrace
                      boost_context
                      boost_stacktrace_backtrace
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
