//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_load_options.hpp>
//
#include <llfs/page_load_options.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

TEST(PageLoadOptionsTest, PageCacheOvercommit)
{
  llfs::PageCacheOvercommit overcommit;
  overcommit.allow(true);
  {
    llfs::PageLoadOptions load_options{llfs::LruPriority{1}, overcommit};
    EXPECT_EQ(std::addressof(overcommit), std::addressof(load_options.overcommit()));
  }
  {
    llfs::PageLoadOptions load_options{overcommit, llfs::LruPriority{1}};
    EXPECT_EQ(std::addressof(overcommit), std::addressof(load_options.overcommit()));
  }
  {
    llfs::PageLoadOptions load_options{};

    EXPECT_NE(std::addressof(overcommit), std::addressof(load_options.overcommit()));
    EXPECT_EQ(std::addressof(llfs::PageCacheOvercommit::not_allowed()),
              std::addressof(load_options.overcommit()));
  }
}

}  // namespace
