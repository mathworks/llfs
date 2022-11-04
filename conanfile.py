#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

from conans import ConanFile, CMake

import os, sys

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
        "gtest/1.11.0",
        "boost/1.79.0",
        "openssl/3.0.3",
        "glog/0.6.0",
        "libunwind/1.5.0",
        "batteries/0.13.2@batteriescpp+batteries/stable",
        "liburing/2.1",
        "cli11/1.9.1",
        "zlib/1.2.13",
    ]
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
        script_dir = os.path.join(os.path.dirname(__file__), 'batteries', 'script')
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

    def build(self):
        cmake = CMake(self)
        cmake.verbose = VERBOSE
        cmake.configure(source_folder="src")
        cmake.build()

    def export(self):
        self.copy("*.sh", src="batteries/script", dst="batteries/script")
        self.copy("*.py", src="batteries/script", dst="batteries/script")

    def package(self):
        self.copy("*.hpp", dst="include", src="src")
        self.copy("*.ipp", dst="include", src="src")
        self.copy("*.sh", src="batteries/script", dst="batteries/script")
        self.copy("*.py", src="batteries/script", dst="batteries/script")
        cmake = CMake(self)
        cmake.configure()
        cmake.install()

    def package_info(self):
        self.cpp_info.cxxflags = ["-std=c++17", "-D_GNU_SOURCE"]
        self.cpp_info.system_libs = ["dl"]
        self.cpp_info.libs = ["llfs"]
