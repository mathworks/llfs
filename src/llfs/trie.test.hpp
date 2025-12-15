//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_TRIE_TEST_HPP

#include <llfs/config.hpp>
//
#include <llfs/testing/test_config.hpp>
#include <llfs/testing/util.hpp>

#include <batteries/assert.hpp>

#include <algorithm>
#include <fstream>
#include <string>
#include <vector>

namespace llfs {
namespace testing {

inline std::vector<std::string> load_words()
{
  std::vector<std::string> words;
  std::string word_file_path = llfs::testing::get_test_data_file_path("words");
  std::ifstream ifs{word_file_path};
  BATT_CHECK(ifs.good()) << BATT_INSPECT_STR(word_file_path);
  std::string word;
  while (ifs.good()) {
    ifs >> word;
    words.emplace_back(word);
  }
  std::sort(words.begin(), words.end());
  words.erase(std::unique(words.begin(), words.end()), words.end());
  return words;
}

}  // namespace testing
}  // namespace llfs
