//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_PACKED_TYPE_TEST_FIXTURE_HPP
#define LLFS_TESTING_PACKED_TYPE_TEST_FIXTURE_HPP

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace llfs {
namespace testing {

class PackedTypeTestFixture : public ::testing::Test
{
 public:
  MutableBuffer mutable_buffer(usize truncate_count = 0)
  {
    truncate_count = std::min(truncate_count, this->buffer_storage.size());
    return MutableBuffer{this->buffer_storage.data(), this->buffer_storage.size() - truncate_count};
  }

  ConstBuffer const_buffer(usize truncate_count = 0)
  {
    truncate_count = std::min(truncate_count, this->buffer_storage.size());
    return ConstBuffer{this->buffer_storage.data(), this->buffer_storage.size() - truncate_count};
  }

  template <typename T>
  auto* pack_into_buffer(T&& obj)
  {
    const usize size = packed_sizeof(obj);
    this->buffer_storage.resize(size);

    {
      DataPacker packer{this->mutable_buffer(1)};
      EXPECT_EQ(pack_object(obj, &packer), nullptr)
          << "packed_sizeof did not return a precise value! (one fewer bytes succeeded)";

      std::memset(this->buffer_storage.data(), 0, this->buffer_storage.size());
    }

    DataPacker packer{this->mutable_buffer()};
    return pack_object(BATT_FORWARD(obj), &packer);
  }

  template <typename T>
  auto unpack_from_buffer(const T& packed)
  {
    this->data_reader.emplace(this->const_buffer());

    return unpack_object(packed, &*this->data_reader);
  }

  std::vector<char> buffer_storage;
  Optional<DataReader> data_reader;
};

}  // namespace testing
}  // namespace llfs

#endif  // LLFS_TESTING_PACKED_TYPE_TEST_FIXTURE_HPP
