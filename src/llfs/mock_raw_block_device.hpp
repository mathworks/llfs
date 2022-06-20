//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MOCK_RAW_BLOCK_DEVICE_HPP
#define LLFS_MOCK_RAW_BLOCK_DEVICE_HPP

#include <llfs/raw_block_device.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace llfs {

class MockRawBlockDevice : public RawBlockDevice
{
 public:
  MOCK_METHOD(StatusOr<i64>, write_some, (i64 offset, const ConstBuffer& data), (override));
  MOCK_METHOD(StatusOr<i64>, read_some, (i64 offset, const MutableBuffer& buffer), (override));
  MOCK_METHOD(StatusOr<i64>, get_size, (), (override));
  MOCK_METHOD(Status, truncate, (i64 /*new_offset_upper_bound*/), (override));
};

}  // namespace llfs

#endif  // LLFS_MOCK_RAW_BLOCK_DEVICE_HPP
