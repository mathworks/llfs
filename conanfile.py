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


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Import batt helper utilities module.
#
sys.path.append(os.path.join(os.path.dirname(__file__), 'script'))
import batt
#
#+++++++++++-+-+--+----- --- -- -  -  -   -


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

    exports = [
        "script/*.py",
        "script/*.sh",
    ]
    exports_sources = [
        "src/CMakeLists.txt",
        "src/**/*.hpp",
        "src/**/*.ipp",
        "src/**/*.cpp",
    ]

    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["boost"].without_test = True
        self.options["batteries"].with_glog = True
        self.options["batteries"].header_only = False


    def requirements(self):
        from batt import VISIBLE, OVERRIDE

        self.requires("batteries/0.49.3", **VISIBLE)
        self.requires("boost/1.83.0", **VISIBLE)
        self.requires("cli11/2.3.2", **VISIBLE)
        self.requires("glog/0.6.0", **VISIBLE)
        self.requires("gtest/1.14.0", **VISIBLE)
        self.requires("libbacktrace/cci.20210118", **VISIBLE)
        self.requires("openssl/3.1.3", **VISIBLE)

        self.requires("zlib/1.2.13", **OVERRIDE)

        if platform.system() == "Linux":
            self.requires("liburing/2.4", **VISIBLE)
            self.requires("libfuse/3.10.5", **VISIBLE)
            self.requires("libunwind/1.7.2", **VISIBLE, **OVERRIDE)

    #+++++++++++-+-+--+----- --- -- -  -  -   -

    from batt import set_version_from_git_tags as set_version
    from batt import cmake_in_src_layout       as layout
    from batt import default_cmake_generate    as generate
    from batt import default_cmake_build       as build
    from batt import default_cmake_lib_package as package
    from batt import default_lib_package_info  as package_info
    from batt import default_lib_package_id    as package_id

    #+++++++++++-+-+--+----- --- -- -  -  -   -
