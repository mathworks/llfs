#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

from conan import ConanFile

import platform


class LlfsConan(ConanFile):
    name = "llfs"

    #----- --- -- -  -  -   -
    # version is set automatically from Git tags - DO NOT SET IT HERE
    #----- --- -- -  -  -   -

    license = "Apache Public License 2.0"

    author = "The MathWorks, Inc."

    url = "https://github.com/mathworks/llfs"

    description = "Low-Level File System Utilities (C++)"

    settings = "os", "compiler", "build_type", "arch"

    options = {
        "shared": [True, False],
    }

    default_options = {
        "shared": False,
    }

    python_requires = "cor_recipe_utils/0.18.2"
    python_requires_extend = "cor_recipe_utils.ConanFileBase"

    tool_requires = [
        "cmake/[>=3.20.0 <4]",
        "ninja/[>=1.12.1 <2]",
    ]

    build_policy = "missing"

    exports_sources = [
        "CMakeLists.txt",
        "src/CMakeLists.txt",
        "src/**/*.hpp",
        "src/**/*.h",
        "src/**/*.ipp",
        "src/**/*.cpp",
    ]

    tool_requires = [
        "cmake/[>=3.20.0 <4]",
        "ninja/[>=1.12.1 <2]",
    ]

    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["boost"].without_test = True
        self.options["batteries"].with_glog = True
        self.options["batteries"].header_only = False


    def requirements(self):
        VISIBLE = self.cor.VISIBLE
        OVERRIDE = self.cor.OVERRIDE

        self.requires("batteries/0.60.2", **VISIBLE)
        self.requires("boost/1.88.0", **VISIBLE, **OVERRIDE)
        self.requires("cli11/2.5.0", **VISIBLE)
        self.requires("glog/0.7.1", **VISIBLE, **OVERRIDE)
        self.requires("libbacktrace/cci.20240730", **VISIBLE, **OVERRIDE)
        self.requires("openssl/3.5.2", **VISIBLE, **OVERRIDE)
        self.requires("xxhash/0.8.3", **VISIBLE)

        self.requires("zlib/1.3.1", **OVERRIDE)

        self.test_requires("gtest/1.16.0")
        
        if platform.system() == "Linux":
            self.requires("liburing/2.11", **VISIBLE)
            self.requires("libfuse/3.16.2", **VISIBLE)
            self.requires("libunwind/1.8.1", **VISIBLE, **OVERRIDE)

    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def set_version(self):
        return self.cor.set_version_from_git_tags(self)

    def layout(self):
        return self.cor.layout_cmake_unified_src(self)

    def generate(self):
        return self.cor.generate_cmake_default(self)

    def build(self):
        return self.cor.build_cmake_default(self)

    def package(self):
        return self.cor.package_cmake_install(self)

    def package_info(self):
        return self.cor.package_info_lib_default(self)

    def package_id(self):
        return self.cor.package_id_lib_default(self)

    #+++++++++++-+-+--+----- --- -- -  -  -   -

