from conans import ConanFile, CMake

import os, sys

VERBOSE_ = os.getenv('VERBOSE') and True or False

SCRIPT_DIR_ = os.path.join(os.path.dirname(__file__), 'batteries', 'script')
if VERBOSE_:
    print(f"SCRIPT_DIR_={SCRIPT_DIR_}", file=sys.stderr)

VERSION_ = os.popen("VERBOSE= " + os.path.join(SCRIPT_DIR_, "get-version.sh")).read().strip()
if VERBOSE_:
    print(f"VERSION_={VERSION_}", file=sys.stderr)

class LlfsConan(ConanFile):
    name = "llfs"
    version = VERSION_
    license = "Apache Public License 2.0"
    author = "The MathWorks, Inc."
    url = "https://gitlab.com/batteriescpp/llfs"
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
        "batteries/0.7.2@tonyastolfi+batteries/stable",
        "liburing/2.1",
        "cli11/1.9.1",
    ]
    exports_sources = [
        "src/CMakeLists.txt",
        "src/**/*.hpp",
        "src/**/*.ipp",
        "src/**/*.cpp",
    ]

    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False

    def build(self):
        cmake = CMake(self)
        cmake.verbose = VERBOSE_
        cmake.configure(source_folder="src")
        cmake.build()

    def package(self):
        self.copy("*.hpp", dst="include", src="src")
        self.copy("*.ipp", dst="include", src="src")
        self.copy("*.sh", dst="bin", src="script")
        cmake = CMake(self)
        cmake.configure()
        cmake.install()

    def package_info(self):
        self.cpp_info.cxxflags = ["-std=c++17", "-D_GNU_SOURCE"]
        self.cpp_info.system_libs = ["dl"]
        self.cpp_info.libs = ["llfs"]

    def package_id(self):
        pass
