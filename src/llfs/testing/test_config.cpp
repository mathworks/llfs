//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/testing/test_config.hpp>
//

#include <batteries/env.hpp>

namespace llfs {
namespace testing {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TestConfig::TestConfig() noexcept
{
  this->extra_testing_ =                          //
      batt::getenv_as<int>("LLFS_EXTRA_TESTING")  //
          .value_or(0);

  this->low_level_log_device_sim_ =                          //
      batt::getenv_as<int>("LLFS_LOW_LEVEL_LOG_DEVICE_SIM")  //
          .value_or(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool TestConfig::extra_testing() const noexcept
{
  return this->extra_testing_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool TestConfig::low_level_log_device_sim() const noexcept
{
  return this->low_level_log_device_sim_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::filesystem::path& TestConfig::project_dir() noexcept
{
  if (this->project_dir_.empty()) {
    this->project_dir_ = []() {
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
  }
  return this->project_dir_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::filesystem::path TestConfig::data_file_path(std::string_view file_rel_path) noexcept
{
  return this->project_dir() / "testdata" / file_rel_path;
}

}  //namespace testing
}  //namespace llfs
