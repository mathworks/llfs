//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/testing/test_config.hpp>
//
#include <llfs/logging.hpp>
#include <llfs/optional.hpp>

#include <batteries/env.hpp>

namespace llfs {
namespace testing {

namespace {

usize get_random_seed_from_env()
{
  static const usize seed = [] {
    const char* varname = "LLFS_TEST_RANDOM_SEED";

    Optional<usize> value = batt::getenv_as<usize>(varname);
    if (!value) {
      LLFS_LOG_INFO() << varname << " not defined; using default value 0";
      return usize{0};
    }
    LLFS_LOG_INFO() << varname << " == " << *value;
    return *value;
  }();

  return seed;
}

}  //namespace

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

  this->random_seed_ = get_random_seed_from_env();
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
      // Preferred method: read the PROJECT_DIR environment variable and use that.
      //
      const char* project_dir_env = std::getenv("PROJECT_DIR");
      if (project_dir_env) {
        return std::filesystem::path{project_dir_env};
      }

      // Fallback method: starting with the path to this source file (__FILE__), recursively look at
      // parent directory names until we find `src`, then go up one level and stop.
      //
      std::filesystem::path file_path{__FILE__};
      while (file_path.has_parent_path() && file_path.filename() != "src") {
        file_path = file_path.parent_path();
      }
      if (file_path.filename() == "src") {
        file_path = file_path.parent_path();
        return file_path;
      }

      // All methods failed.
      //
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize TestConfig::get_random_seed() noexcept
{
  return this->random_seed_;
}

}  //namespace testing
}  //namespace llfs
