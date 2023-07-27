//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/fuse.hpp>
//
#include <llfs/fuse.hpp>
#include <llfs/null_fuse_impl.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

TEST(FuseTest, Test)
{
  const fuse_lowlevel_ops* ops = llfs::NullFuseImpl::get_fuse_lowlevel_ops();

  ASSERT_NE(ops, nullptr);
}

}  // namespace
