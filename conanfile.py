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

import os, sys, platform, traceback

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

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _append_script_dir(self):
        newline = "\n      "
        try:
            print(
                f'BEFORE modifying sys.path:' +
                f'\n  cwd={os.getcwd()}' +
                f'\n  sys.path={newline + newline.join(sys.path)}' +
                f'\n  source_folder={self.source_folder}' +
                f'\n  __file__={__file__}' +
                f'\n  stack=', file=sys.stderr
            )
            traceback.print_stack(file=sys.stderr)
        
            script_dir1 = os.path.join(os.path.dirname(__file__), 'script')
            sys.path.append(script_dir1)

            try:
                script_dir2 = os.path.join(os.path.split(self.source_folder)[0], 'script')
                sys.path.append(script_dir2)
            except:
                pass
            
        finally:
            print(
                f'\nAFTER modifying sys.path:' +
                f'\n  cwd={os.getcwd()}' +
                f'\n  sys.path={newline + newline.join(sys.path)}', file=sys.stderr
            )
            pass
            
    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def set_version(self):
        self._append_script_dir()
        import batt
        batt.VERBOSE = VERBOSE
        self.version = batt.get_version(no_check_conan=True)
        batt.verbose(f'VERSION={self.version}')


    def requirements(self):
        deps = [
            "batteries/0.49.1",
            "boost/1.83.0",
            "cli11/2.3.2",
            "glog/0.6.0",
            "gtest/1.14.0",
            "libbacktrace/cci.20210118",
            "openssl/3.1.3",
        ]

        override_deps = [
            "zlib/1.2.13",
        ]

        platform_deps = {
            "Linux": [
                "liburing/2.4",
                "libfuse/3.10.5",
            ]
        }

        if platform.system() == 'Linux':
            self.requires("libunwind/1.7.2",
                          override=True,
                          visible=True,
                          transitive_headers=True,
                          transitive_libs=True)

        self._append_script_dir()
        import batt
        batt.conanfile_requirements(self, deps, override_deps, platform_deps)


    def layout(self):
        cmake_layout(self, src_folder="src")


    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

        self._append_script_dir()
        import batt
        batt.VERBOSE = VERBOSE
        batt.generate_conan_find_requirements(self)


    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["boost"].without_test = True
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
