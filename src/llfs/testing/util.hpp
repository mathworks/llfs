//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_UTIL_HPP
#define LLFS_TESTING_UTIL_HPP

#include <llfs/config.hpp>
//
#include <llfs/optional.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>

namespace llfs {
namespace testing {

/** \brief Returns the source project root directory.  This is the top-level GIT repo dir.
 *
 * First the environment variable PROJECT_DIR is checked; if this fails, then this function attempts
 * to locate this source file using __FILE__ and work backwards from there.
 */
const std::filesystem::path& get_project_dir();

/** \brief Returns the full path to the specified test data file, specified as a relative path with
 * the testdata/ dir as its base.
 */
std::filesystem::path get_test_data_file_path(std::string_view file_rel_path);

}  //namespace testing
}  //namespace llfs

#endif  // LLFS_TESTING_UTIL_HPP
