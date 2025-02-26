//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_TEST_CONFIG_HPP
#define LLFS_TESTING_TEST_CONFIG_HPP

#include <llfs/config.hpp>
//

#include <filesystem>

namespace llfs {
namespace testing {

/** \brief Captures testing-related settings from the environment and presents them to test code
 * with a simple API.
 */
class TestConfig
{
 public:
  /** \brief Construct a TestConfig from the current environment.
   */
  TestConfig() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns whether tests should take extra time to run very thorough tests on more cases.
   *
   * Controlled by env var `LLFS_EXTRA_TESTING`.
   */
  bool extra_testing() const noexcept;

  /** \brief Returns whether StorageSimulations should use low-level LogDevice simulation
   * (default=true).
   *
   * Controlled by env var `LLFS_LOW_LEVEL_LOG_DEVICE_SIM`.
   */
  bool low_level_log_device_sim() const noexcept;

  /** \brief Determines the current repository root directory.
   *
   * By default, this is derived from `__FILE__` and built-in assumptions about the directory layout
   * of the project.  It can be overridden via the environment variable `PROJECT_DIR`.
   */
  const std::filesystem::path& project_dir() noexcept;

  /** \brief Returns the path to the named data file.
   *
   * Test data files are located in <PROJECT_DIR>/testdata/.
   */
  std::filesystem::path data_file_path(std::string_view file_rel_path) noexcept;

  /** \brief Returns initial random seed for tests that use pseudo-random number generators.
   *
   * Tries to read from env var `LLFS_TEST_RANDOM_SEED`; defaults to 0.
   */
  usize get_random_seed() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  bool extra_testing_;

  bool low_level_log_device_sim_;

  std::filesystem::path project_dir_;

  usize random_seed_;
};

}  //namespace testing
}  //namespace llfs

#endif  // LLFS_TESTING_TEST_CONFIG_HPP
