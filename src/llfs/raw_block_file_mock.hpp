//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_RAW_BLOCK_FILE_MOCK_HPP
#define LLFS_RAW_BLOCK_FILE_MOCK_HPP

#include <llfs/raw_block_file.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace llfs {

class RawBlockFileMock : public RawBlockFile
{
 public:
  MOCK_METHOD(StatusOr<i64>, write_some, (i64 offset, const ConstBuffer& data), (override));
  MOCK_METHOD(StatusOr<i64>, read_some, (i64 offset, const MutableBuffer& buffer), (override));
  MOCK_METHOD(StatusOr<i64>, get_size, (), (override));
  MOCK_METHOD(Status, truncate, (i64 new_offset_upper_bound), (override));
  MOCK_METHOD(Status, truncate_at_least, (i64 minimum_size), (override));
};

}  // namespace llfs

#endif  // LLFS_RAW_BLOCK_FILE_MOCK_HPP
