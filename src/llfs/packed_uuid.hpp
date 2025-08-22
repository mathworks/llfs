//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_UUID_HPP
#define LLFS_PACKED_UUID_HPP

#include <llfs/config.hpp>
//

#include <llfs/int_types.hpp>
#include <llfs/uuid.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/version.hpp>

#include <cstring>

namespace llfs {

struct PackedUUID {
  std::array<u8, sizeof(boost::uuids::uuid)> bytes;

  //----- --- -- -  -  -   -

  PackedUUID() noexcept
  {
  }

  /*implicit*/ PackedUUID(const boost::uuids::uuid& that) noexcept
  {
    *this = that;
  }

  PackedUUID& operator=(const boost::uuids::uuid& that) noexcept
  {
    std::memcpy(this->bytes.data(), &that, this->bytes.size());
    return *this;
  }

#if BOOST_VERSION < 108600

  operator const boost::uuids::uuid&() const noexcept
  {
    BATT_STATIC_ASSERT_EQ(alignof(boost::uuids::uuid), 1);

    return *reinterpret_cast<const boost::uuids::uuid*>(this->bytes.data());
  }

#else

  operator boost::uuids::uuid() const noexcept
  {
    return boost::uuids::uuid{(const u8(&)[16])this->bytes};
  }

#endif
};

namespace {
BATT_STATIC_ASSERT_EQ(sizeof(PackedUUID), 16);
BATT_STATIC_ASSERT_EQ(alignof(PackedUUID), 1);
BATT_STATIC_ASSERT_EQ(sizeof(boost::uuids::uuid), sizeof(PackedUUID));
}  //namespace

inline std::ostream& operator<<(std::ostream& out, const PackedUUID& t)
{
  return out << (const boost::uuids::uuid&)t;
}

}  //namespace llfs

#endif  // LLFS_PACKED_UUID_HPP
