//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/testing/util.hpp>
//

#include <llfs/testing/test_config.hpp>

namespace llfs {
namespace testing {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::filesystem::path& get_project_dir()
{
  TestConfig test_config;

  return test_config.project_dir();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::filesystem::path get_test_data_file_path(std::string_view file_rel_path)
{
  TestConfig test_config;

  return test_config.data_file_path(file_rel_path);
}

}  //namespace testing
}  //namespace llfs
