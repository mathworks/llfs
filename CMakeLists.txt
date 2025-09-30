##=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https:#www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

cmake_minimum_required(VERSION 3.20)
project(llfs CXX)

include(${COR_CMAKE_INCLUDE_DIR}/common.cmake)
include(${CMAKE_BINARY_DIR}/conan_find_requirements.cmake)

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Do not attempt to set/fix rpath for binaries.
#
set(CMAKE_SKIP_INSTALL_RPATH TRUE)

# Turn off terminal color codes in gcc.
#
add_definitions("-fno-diagnostics-color")

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
if(APPLE)
  set(BACKTRACE_DEPENDENCY "")
else()
  set(BACKTRACE_DEPENDENCY "backtrace")
endif()

include_directories(${GTest_INCLUDE_DIRS})

# Configure libfuse.
#
include_directories(${libfuse_INCLUDE_DIRS})
add_compile_definitions(FUSE_USE_VERSION=35)

# Configure Google Logging.
#
add_compile_definitions(GLOG_CUSTOM_PREFIX_SUPPORT=1)

add_custom_target(RunAllLibTests)

add_subdirectory(src)
