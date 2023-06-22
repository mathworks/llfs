#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.files import copy

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
    build_policy = "missing"

    exports_sources = [
        "src/CMakeLists.txt",
        "src/**/*.hpp",
        "src/**/*.ipp",
        "src/**/*.cpp",
        "script/*.py",
        "script/*.sh",
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


    def requirements(self):
        deps = [
            "libbacktrace/cci.20210118",
            "gtest/1.13.0",
            "boost/1.82.0",
            "glog/0.6.0",
            "batteries/0.38.10",
            "cli11/2.3.2",
        ]

        override_deps = [
            "openssl/3.1.0",
            "zlib/1.2.13",
        ]

        platform_deps = {
            "Linux": [
                "libunwind/1.6.2",
                "liburing/2.4",
            ]
        }

        if platform.system() in platform_deps:
            deps += platform_deps[platform.system()]

        for dep_name in deps:
            self.requires(dep_name,
                          visible=True,
                          transitive_headers=True,
                          transitive_libs=True)

        for override_name in override_deps:
            self.requires(override_name,
                          override=True)

        self.build_requires("b2/4.10.0")


    def layout(self):
        cmake_layout(self, src_folder="src")


    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()


    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["batteries"].with_glog = True
        self.options["batteries"].header_only = False


    def build(self):
        cmake = CMake(self)
        cmake.verbose = VERBOSE
        cmake.configure()
        cmake.build()


    def package(self):
        src_include = os.path.join(self.source_folder, ".")
        dst_include = os.path.join(self.package_folder, "include")

        copy(self, "*.hpp", dst=dst_include, src=src_include)
        copy(self, "*.ipp", dst=dst_include, src=src_include)

        cmake = CMake(self)
        cmake.configure()
        cmake.install()


    def package_info(self):
        self.cpp_info.cxxflags = ["-D_GNU_SOURCE", "-D_BITS_UIO_EXT_H=1"]
        self.cpp_info.system_libs = ["dl"]
        self.cpp_info.libs = ["llfs"]
