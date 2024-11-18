//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/stable_string_store.hpp>
//

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

TEST(StableStringStore, Test)
{
  llfs::StableStringStore strings;

  auto out =
      strings.concat(std::string_view{"Hello"}, std::string_view{", "}, std::string_view{"World!"});

  std::string_view out_str{(const char*)out.data(), out.size()};

  EXPECT_THAT(out_str, ::testing::StrEq("Hello, World!"));
}

}  // namespace
