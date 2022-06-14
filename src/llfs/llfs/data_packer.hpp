#pragma once
#ifndef LLFS_DATA_PACKER_HPP
#define LLFS_DATA_PACKER_HPP

#include <llfs/array_packer.hpp>
#include <llfs/buffer.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/interval.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_variant.hpp>
#include <llfs/seq.hpp>
#include <llfs/slice.hpp>

#include <batteries/type_traits.hpp>

#include <boost/range/iterator_range.hpp>

#include <cstddef>

namespace llfs {

class DataPacker
{
 public:
  template <typename T>
  using ArrayPacker = BasicArrayPacker<T, DataPacker>;

  explicit DataPacker(const MutableBuffer& buffer) noexcept;

  DataPacker(const DataPacker&) = delete;
  DataPacker& operator=(const DataPacker&) = delete;

  DataPacker(DataPacker&& that) noexcept;
  DataPacker& operator=(DataPacker&& that) noexcept;

  void invalidate();

  explicit operator bool() const
  {
    return buffer_.data() != nullptr;
  }

  usize space() const
  {
    return avail_.size();
  }

  usize size() const
  {
    return buffer_.size() - this->space();
  }

  bool full() const
  {
    return full_;
  }

  void reset_flags()
  {
    full_ = false;
  }

  template <typename T>
  bool contains(T* rec) const
  {
    const u8* rec_begin = reinterpret_cast<const u8*>(rec);
    const u8* rec_end = rec_begin + sizeof(T);

    return (rec_begin >= this->buffer_begin()) &&  //
           (rec_end <= this->buffer_end());
  }

  template <typename T>
  [[nodiscard]] T* pack_record(const batt::StaticType<T>& = {})
  {
    if (full_ || space() < sizeof(T)) {
      full_ = true;
      return nullptr;
    }
    T* s = reinterpret_cast<T*>(avail_.begin());
    avail_.advance_begin(sizeof(T));
    return s;
  }

  [[nodiscard]] const void* pack_data(const void* data, usize size);

  [[nodiscard]] const void* pack_data_to(PackedBytes* rec, const void* data, usize size);

  [[nodiscard]] const PackedBytes* pack_data_copy(const PackedBytes& src);

  [[nodiscard]] const PackedBytes* pack_data_copy_to(PackedBytes* dst, const PackedBytes& src);

  [[nodiscard]] Optional<std::string_view> pack_string(const std::string_view& s);

  [[nodiscard]] Optional<std::string_view> pack_string_to(PackedBytes* rec,
                                                          const std::string_view& s);

  template <typename U>
  [[nodiscard]] bool pack_u64(U val)
  {
    static_assert(std::is_same_v<std::decay_t<U>, u64>, "Must be called with u64");

    if (full_ || space() < sizeof(little_u64)) {
      full_ = true;
      return false;
    }
    *pack_record<little_u64>() = val;
    return true;
  }

  template <typename U>
  [[nodiscard]] bool pack_u32(U val)
  {
    static_assert(std::is_same_v<std::decay_t<U>, u32>, "Must be called with u32");

    if (full_ || space() < sizeof(little_u32)) {
      full_ = true;
      return false;
    }
    *pack_record<little_u32>() = val;
    return true;
  }

  template <typename U>
  [[nodiscard]] bool pack_i32(U val)
  {
    static_assert(std::is_same_v<std::decay_t<U>, i32>, "Must be called with i32");

    if (full_ || space() < sizeof(little_i32)) {
      full_ = true;
      return false;
    }
    *pack_record<little_i32>() = val;
    return true;
  }

  template <typename U>
  [[nodiscard]] bool pack_u16(U val)
  {
    static_assert(std::is_same_v<std::decay_t<U>, u16>, "Must be called with u16");

    if (full_ || space() < sizeof(little_u16)) {
      full_ = true;
      return false;
    }
    *pack_record<little_u16>() = val;
    return true;
  }

  template <typename T>
  Optional<ArrayPacker<T>> pack_array()
  {
    PackedArray<T>* array = this->pack_record<PackedArray<T>>();
    if (!array || full_) {
      return None;
    }
    array->initialize(0u);
    return ArrayPacker<T>{array, this};
  }

  template <typename T = void, typename Seq,
            typename R = std::conditional_t<std::is_same_v<T, void>, SeqItem<Seq>, T>>
  Optional<ArrayPacker<R>> pack_seq(Seq&& seq)
  {
    if (full_ || this->space() < packed_array_size<R>(batt::make_copy(seq) | seq::count())) {
      DLOG(INFO) << "pack_seq - space check failed";
      return None;
    }

    PackedArray<R>* array = this->pack_record<PackedArray<R>>();
    BATT_CHECK_NOT_NULLPTR(array);
    array->initialize(0u);
    ArrayPacker<R> array_packer{array, this};

    BATT_CHECK(array_packer.pack_seq(BATT_FORWARD(seq)));

    return array_packer;
  }

  template <typename Range, typename T = typename Range::value_type>
  Optional<ArrayPacker<T>> pack_range(const Range& r)
  {
    auto first = std::begin(r);
    auto last = std::end(r);
    auto item_count = std::distance(first, last);

    if (full_ || this->space() < packed_array_size<T>(item_count)) {
      return None;
    }
    PackedArray<T>* array = this->pack_record<PackedArray<T>>();
    BATT_CHECK_NOT_NULLPTR(array);
    array->initialize(item_count);
    std::copy(first, last, array->begin());
    avail_.advance_begin(sizeof(T) * item_count);

    return ArrayPacker<T>{array, this};
  }

  template <typename... Ts, typename T>
  PackedVariantInstance<PackedVariant<Ts...>, T>* pack_variant(
      batt::StaticType<PackedVariant<Ts...>>, batt::StaticType<T>)
  {
    auto* instance =
        this->pack_record(batt::StaticType<PackedVariantInstance<PackedVariant<Ts...>, T>>{});
    if (!instance) {
      return nullptr;
    }

    // This will set the `which` field of the variant record.
    //
    instance->init();

    return instance;
  }

  u8* pack_varint(u64 n);

  Interval<std::ptrdiff_t> unused() const
  {
    return Interval<std::ptrdiff_t>{this->avail_.begin() - this->buffer_begin(),
                                    this->avail_.end() - this->buffer_begin()};
  }

  usize buffer_size() const
  {
    return this->buffer_.size();
  }

  MutableBuffer avail_buffer() const
  {
    return MutableBuffer{this->avail_.begin(), this->avail_.size()};
  }

  u8* buffer_begin() const
  {
    return static_cast<u8*>(this->buffer_.data());
  }
  u8* buffer_end() const
  {
    return this->buffer_begin() + this->buffer_.size();
  }

 private:
  usize estimate_packed_data_size(usize size) const;

  usize estimate_packed_data_size(const PackedBytes& src) const;

  const void* nocheck_pack_data_to(PackedBytes* rec, const void* data, usize size);

  // Copy data into newly "allocated" space at the end of the avail range.
  //
  // Requires that:
  //  - size >= 4
  //  - size <= this->space()
  //
  const void* nocheck_pack_data_large(PackedBytes* dst, const void* data, usize size);

  const PackedBytes* nocheck_pack_data_copy_to(PackedBytes* dst, const PackedBytes& src);

  MutableBuffer buffer_;
  boost::iterator_range<u8*> avail_{this->buffer_begin(), this->buffer_end()};
  bool full_ = false;
};

}  // namespace llfs

#endif  // LLFS_DATA_PACKER_HPP
