//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_BUFFER_VIEW_TEST_HPP
#define LLFS_IORING_BUFFER_VIEW_TEST_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/ioring_buffer_view.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace llfs {
namespace testing {

class IoringBufferViewTest : public ::testing::Test
{
 public:
  static constexpr usize kTestBufferCount = 4;
  static constexpr usize kTestBufferSize = 4096;

  void SetUp() override
  {
    this->scoped_io_ring =
        llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{8}, llfs::ThreadPoolSize{1});

    ASSERT_TRUE(this->scoped_io_ring.ok()) << BATT_INSPECT(this->scoped_io_ring.status());

    this->io_ring = std::addressof(this->scoped_io_ring->get_io_ring());

    this->status_or_buffer_pool = llfs::IoRingBufferPool::make_new(
        *this->io_ring, llfs::BufferCount{kTestBufferCount}, llfs::BufferSize{kTestBufferSize});

    ASSERT_TRUE(this->status_or_buffer_pool.ok())
        << BATT_INSPECT(this->status_or_buffer_pool.status());

    this->buffer_pool = this->status_or_buffer_pool->get();

    this->buffer_1 = this->buffer_pool->await_allocate();

    ASSERT_TRUE(this->buffer_1.ok()) << BATT_INSPECT(this->buffer_1.status());
  }

  llfs::StatusOr<llfs::ScopedIoRing> scoped_io_ring;

  const llfs::IoRing* io_ring = nullptr;

  llfs::StatusOr<std::unique_ptr<llfs::IoRingBufferPool>> status_or_buffer_pool;

  llfs::IoRingBufferPool* buffer_pool = nullptr;

  llfs::StatusOr<llfs::IoRingBufferPool::Buffer> buffer_1;
};

}  //namespace testing
}  //namespace llfs

#endif  // LLFS_IORING_BUFFER_VIEW_TEST_HPP
