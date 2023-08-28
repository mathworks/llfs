//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/testing/util.hpp>
//

namespace llfs {
namespace testing {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::filesystem::path& get_project_dir()
{
  static const std::filesystem::path cached = []() {
    const char* project_dir_env = std::getenv("PROJECT_DIR");
    if (project_dir_env) {
      return std::filesystem::path{project_dir_env};
    }

    std::filesystem::path file_path{__FILE__};
    if (std::filesystem::exists(file_path)) {
      std::filesystem::path testing_dir_path = file_path.parent_path();
      if (std::filesystem::exists(testing_dir_path)) {
        std::filesystem::path src_llfs_dir_path = testing_dir_path.parent_path();
        if (std::filesystem::exists(src_llfs_dir_path)) {
          std::filesystem::path src_dir_path = src_llfs_dir_path.parent_path();
          if (std::filesystem::exists(src_dir_path)) {
            std::filesystem::path project_dir_path = src_dir_path.parent_path();
            if (std::filesystem::exists(project_dir_path)) {
              return project_dir_path;
            }
          }
        }
      }
    }

    BATT_PANIC() << "Could not find project root; make sure PROJECT_DIR is set!";
    BATT_UNREACHABLE();
  }();

  return cached;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::filesystem::path get_test_data_file_path(std::string_view file_rel_path)
{
  return get_project_dir() / "testdata" / file_rel_path;
}

}  //namespace testing
}  //namespace llfs
