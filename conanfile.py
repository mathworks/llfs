#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

from conans import ConanFile, CMake

import os, sys, platform

VERBOSE = os.getenv('VERBOSE') and True or False


class LlfsConan(ConanFile):
    name = "llfs"
    # version is set automatically from Git tags - DO NOT SET IT HERE
    license = "Apache Public License 2.0"
    author = "The MathWorks, Inc."
    url = "https://github.com/mathworks/llfs"
    description = "Low-Level File System Utilities (C++)"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = {"shared": False}
    generators = "cmake"
    build_policy = "missing"

    requires = [
        "libbacktrace/cci.20210118",
        "b2/4.9.6",
        "gtest/1.13.0",
        "boost/1.81.0",
        "glog/0.6.0",
        "batteries/0.34.11@batteriescpp+batteries/stable",
        "cli11/2.3.2",

        # Version overrides (conflict resolutions)
        #
        ("openssl/3.1.0", "override"),
        ("zlib/1.2.13", "override"),
    ] + ([
        "libunwind/1.6.2",
        "liburing/2.2",
    ] if platform.system() == 'Linux' else [])

    exports_sources = [
        "src/CMakeLists.txt",
        "src/**/*.hpp",
        "src/**/*.ipp",
        "src/**/*.cpp",
    ]

    def set_version(self):
        #==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
        # Import the Batteries utility module.
        #
        script_dir = os.path.join(os.path.dirname(__file__), 'script')
        sys.path.append(script_dir)
        
        import batt
        
        batt.VERBOSE = VERBOSE
        self.version = batt.get_version(no_check_conan=True)
        batt.verbose(f'VERSION={self.version}')
        #
        #+++++++++++-+-+--+----- --- -- -  -  -   -
    
    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["batteries"].with_glog = True
        self.options["batteries"].header_only = False

    def build(self):
        cmake = CMake(self)
        cmake.verbose = VERBOSE
        cmake.configure(source_folder="src")
        cmake.build()

    def export(self):
        self.copy("*.sh", src="script", dst="script")
        self.copy("*.py", src="script", dst="script")

    def package(self):
        self.copy("*.hpp", dst="include", src="src")
        self.copy("*.ipp", dst="include", src="src")
        self.copy("*.sh", src="script", dst="script")
        self.copy("*.py", src="script", dst="script")
        cmake = CMake(self)
        cmake.configure()
        cmake.install()

    def package_info(self):
        self.cpp_info.cxxflags = ["-std=c++17", "-D_GNU_SOURCE", "-D_BITS_UIO_EXT_H=1"]
        self.cpp_info.system_libs = ["dl"]
        self.cpp_info.libs = ["llfs"]
