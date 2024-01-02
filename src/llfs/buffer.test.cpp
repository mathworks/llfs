//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/buffer.hpp>
//
#include <llfs/buffer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace llfs::int_types;

TEST(BufferTest, ResizeBufferStorage)
{
  std::unique_ptr<u8> storage;
  llfs::MutableBuffer buffer = resize_buffer_storage(storage, 1977);

  EXPECT_NE(buffer.data(), nullptr);
  EXPECT_EQ((void*)buffer.data(), (void*)storage.get());
  EXPECT_EQ(buffer.size(), 1977u);
}

}  // namespace
