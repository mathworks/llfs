//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_DATA_READER_HPP
#define LLFS_DATA_READER_HPP

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Workaround - don't change!
//
#include <batteries/stream_util.hpp>
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/buffer.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_bytes.hpp>
#include <llfs/packed_variant.hpp>

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>
#include <batteries/type_traits.hpp>

#include <boost/range/iterator_range.hpp>

#include <cstddef>
#include <string_view>

namespace llfs {

class DataReader
{
 public:
  explicit DataReader(const ConstBuffer& buffer) noexcept : buffer_{buffer}
  {
  }

  explicit DataReader(const std::string_view& str) noexcept
      : DataReader{ConstBuffer{str.data(), str.size()}}
  {
  }

  DataReader(DataReader&& that) noexcept
      : at_end_{that.at_end_}
      , buffer_{that.buffer_}
      , unread_{that.unread_}
  {
    that.invalidate();
  }

  DataReader& operator=(DataReader&& that) noexcept
  {
    if (BATT_HINT_TRUE(this != &that)) {
      this->buffer_ = that.buffer_;
      this->unread_ = that.unread_;
      this->at_end_ = that.at_end_;
      that.invalidate();
    }
    return *this;
  }

  void invalidate()
  {
    this->buffer_ = ConstBuffer{nullptr, 0};
    this->unread_ = boost::make_iterator_range<u8*>(nullptr, nullptr);
    this->at_end_ = true;
  }

  explicit operator bool() const
  {
    return buffer_.data() != nullptr;
  }

  usize bytes_consumed() const
  {
    return this->buffer_.size() - this->bytes_available();
  }

  usize bytes_available() const
  {
    return unread_.size();
  }

  bool at_end() const
  {
    return at_end_;
  }

  void reset_flags()
  {
    at_end_ = false;
  }

  template <typename T>
  [[nodiscard]] const T* read_record(batt::StaticType<T> = {})
  {
    if (at_end_ || this->bytes_available() < sizeof(T)) {
      at_end_ = true;
      return nullptr;
    }
    const T* s = reinterpret_cast<const T*>(unread_.begin());
    unread_.advance_begin(sizeof(T));
    return s;
  }

  // TODO [tastolfi 2020-12-04] - should this return ConstBuffer?
  //
  [[nodiscard]] boost::iterator_range<const u8*> read_data()
  {
    if (at_end_ || unread_.size() < sizeof(PackedBytes)) {
      at_end_ = true;
      return {nullptr, nullptr};
    }

    auto* rec = read_record<PackedBytes>();
    BATT_ASSERT_NOT_NULLPTR(rec);

    if (rec->data_offset < sizeof(PackedBytes)) {
      BATT_CHECK_GE(rec->data_offset.value(), 4);
      return rec->bytes_range();
    }

    const u8* before = unread_.end();
    unread_.advance_end(-std::ptrdiff_t(rec->data_size.value()));

    const u8* data_begin = unread_.end();

    BATT_CHECK_LE(data_begin, before);
    BATT_CHECK_EQ(before - data_begin, rec->data_size);

    const u8* data_end = data_begin + rec->data_size;

    BATT_CHECK_EQ((const void*)(data_begin + rec->data_size), (const void*)before)
        << " data_size=" << rec->data_size;

    BATT_CHECK_EQ((const void*)data_begin,
                  (const void*)(reinterpret_cast<const u8*>(rec) + rec->data_offset))
        << "data_offset=" << rec->data_offset << " data_size=" << rec->data_size
        << " before=" << (const void*)before << " data_end=" << (const void*)data_end;

    return {data_begin, data_end};
  }

  [[nodiscard]] Optional<std::string_view> read_string()
  {
    auto d = read_data();
    if (at_end_ || (d.begin() == nullptr && d.end() == nullptr)) {
      at_end_ = true;
      return None;
    }
    std::string_view s(reinterpret_cast<const char*>(d.begin()), d.size());

    BATT_CHECK_GE((const void*)s.data(), (const void*)this->buffer_begin());
    BATT_CHECK_LE((const void*)(s.data() + s.size()), (const void*)this->buffer_end());

    return s;
  }

  [[nodiscard]] Optional<u64> read_u64()
  {
    if (at_end_ || this->bytes_available() < sizeof(little_u64)) {
      at_end_ = true;
      return None;
    }
    return {*read_record<little_u64>()};
  }

  [[nodiscard]] Optional<u32> read_u32()
  {
    if (at_end_ || this->bytes_available() < sizeof(little_u32)) {
      at_end_ = true;
      return None;
    }
    return {*read_record<little_u32>()};
  }

  [[nodiscard]] Optional<i32> read_i32()
  {
    if (at_end_ || this->bytes_available() < sizeof(little_i32)) {
      at_end_ = true;
      return None;
    }
    return {*read_record<little_i32>()};
  }

  [[nodiscard]] Optional<u16> read_u16()
  {
    if (at_end_ || this->bytes_available() < sizeof(little_u16)) {
      at_end_ = true;
      return None;
    }
    return {*read_record<little_u16>()};
  }

  template <typename... Ts, typename Fn>
  [[nodiscard]] bool read_variant(batt::StaticType<PackedVariant<Ts...>> t, Fn&& visitor_fn)
  {
    const auto* head = this->read_record(t);
    if (!head) {
      return false;
    }
    return head->visit_type([&visitor_fn, head, this](auto u) {
      const auto* tail = this->read_record(u);
      if (!tail) {
        return false;
      }
      BATT_FORWARD(visitor_fn)(*head, *tail);
      return true;
    });
  }

  Optional<u64> read_varint();

  const u8* buffer_begin() const
  {
    return static_cast<const u8*>(this->buffer_.data());
  }
  const u8* buffer_end() const
  {
    return this->buffer_begin() + this->buffer_.size();
  }

  std::string_view read_raw(usize max_byte_count)
  {
    const usize n_read = std::min(this->bytes_available(), max_byte_count);
    std::string_view result{(const char*)this->unread_.begin(), n_read};
    this->unread_.advance_end(n_read);
    return result;
  }

  ConstBuffer unread_data() const
  {
    return ConstBuffer{this->unread_.begin(), this->unread_.size()};
  }

 private:
  bool at_end_ = false;
  ConstBuffer buffer_;
  boost::iterator_range<const u8*> unread_{buffer_begin(), buffer_end()};
};

}  // namespace llfs

#endif  // LLFS_DATA_READER_HPP
