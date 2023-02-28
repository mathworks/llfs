//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

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

#include <batteries/async/worker_pool.hpp>
#include <batteries/optional.hpp>
#include <batteries/pointers.hpp>
#include <batteries/type_traits.hpp>

#include <boost/range/iterator_range.hpp>

#include <cstddef>

namespace llfs {

class DataPacker
{
 public:
  template <typename T>
  using ArrayPacker = BasicArrayPacker<T, DataPacker>;

  struct AllocFrontPolicy;
  struct AllocBackPolicy;

  /*! \brief A sub-region of the buffer used for allocation/reservation.
   *
   * Arena is movable but not copyable.
   */
  class Arena
  {
    friend class DataPacker;

   public:
    //----- --- -- -  -  -   -
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    Arena(Arena&&) = default;
    Arena& operator=(Arena&&) = default;
    //----- --- -- -  -  -   -

    /*! \brief The number of bytes available in this Arena for reservation/allocation.
     */
    usize space() const;

    /*! \brief The original size of this arena.
     */
    usize capacity() const;

    /*! \brief Returns true iff some prior reserve or allocate has failed
     */
    bool full() const;

    /*! \brief Returns the available region of this arena as offsets relative to the packer's
     * buffer.
     */
    Interval<isize> unused() const;

    /*! \brief Clears this object, detaching it from its buffer and packer.
     */
    void invalidate();

    /*! \brief Set the `full` flag to true, disabling future reservation/allocation.
     */
    void set_full();

    /*! \brief Split this arena by removing `size` bytes from the front of the available region.
     */
    Optional<Arena> reserve_front(usize size);

    /*! \brief Split this arena by removing `size` bytes from the back of the available region.
     */
    Optional<Arena> reserve_back(usize size);

    /*! \brief Allocate `size` bytes at the front of the available region.
     */
    Optional<MutableBuffer> allocate_front(usize size);

    /*! \brief Allocate `size` bytes at the back of the available region.
     */
    Optional<MutableBuffer> allocate_back(usize size);

    /*! \brief Pack `n` as a variable-length integer at the front of this arena.
     *
     * \return nullptr if there isn't enough space to pack n; otherwise return pointer to the first
     * byte of the packed var-int.
     */
    u8* pack_varint(u64 n);

   private:
    explicit Arena(DataPacker* packer, boost::iterator_range<u8*> avail) noexcept;

    /*! \brief The DataPacker object with which this Arena is associated, for sanity checking.
     */
    DataPacker* get_packer() const;

    boost::iterator_range<u8*> nocheck_alloc_front(isize size);

    boost::iterator_range<u8*> nocheck_alloc_back(isize size);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    usize capacity_;
    batt::UniqueNonOwningPtr<DataPacker> packer_;
    boost::iterator_range<u8*> avail_;
    bool full_ = false;
  };

  /*! \brief Allocates from Arena in front-to-back order.
   */
  struct AllocFrontPolicy {
    static Optional<MutableBuffer> allocate_buffer(Arena* arena, usize size)
    {
      return arena->allocate_front(size);
    }

    static boost::iterator_range<u8*> nocheck_alloc(Arena* arena, isize size)
    {
      return arena->nocheck_alloc_front(size);
    }
  };

  /*! \brief Allocates from Arena in back-to-front order.
   */
  struct AllocBackPolicy {
    static Optional<MutableBuffer> allocate_buffer(Arena* arena, usize size)
    {
      return arena->allocate_back(size);
    }

    static boost::iterator_range<u8*> nocheck_alloc(Arena* arena, isize size)
    {
      return arena->nocheck_alloc_back(size);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
    return this->arena_.space();
  }

  usize size() const
  {
    return this->buffer_.size() - this->space();
  }

  bool full() const
  {
    return this->arena_.full();
  }

  void set_full()
  {
    this->arena_.set_full();
  }

  void reset_flags_DEPRECATED()
  {
    // TODO [tastolfi 2022-09-09] figure this out...
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
    Optional<MutableBuffer> buf = this->arena_.allocate_front(sizeof(T));
    if (!buf) {
      return nullptr;
    }
    return reinterpret_cast<T*>(buf->data());
  }

  /*! \brief Reserve space at the end of the buffer for later allocation.
   *
   * DataPacker functions that allocate trailing buffer space (pack_data, pack_string, etc.) can be
   * passed a DataPacker::Arena to allocate portions of the reserved space later.
   */
  [[nodiscard]] Optional<Arena> reserve_arena(usize size);

  [[nodiscard]] const void* pack_data(const void* data, usize size);
  [[nodiscard]] const void* pack_data(const void* data, usize size, Arena* arena);

  [[nodiscard]] const void* pack_data_to(PackedBytes* rec, const void* data, usize size);
  [[nodiscard]] const void* pack_data_to(PackedBytes* rec, const void* data, usize size,
                                         Arena* arena);

  [[nodiscard]] const PackedBytes* pack_data_copy(const PackedBytes& src);
  [[nodiscard]] const PackedBytes* pack_data_copy(const PackedBytes& src, Arena* arena);

  [[nodiscard]] const PackedBytes* pack_data_copy_to(PackedBytes* dst, const PackedBytes& src);
  [[nodiscard]] const PackedBytes* pack_data_copy_to(PackedBytes* dst, const PackedBytes& src,
                                                     Arena* arena);

  [[nodiscard]] Optional<std::string_view> pack_string(const std::string_view& s);
  [[nodiscard]] Optional<std::string_view> pack_string(const std::string_view& s, Arena* arena);

  [[nodiscard]] Optional<std::string_view> pack_string_to(PackedBytes* rec,
                                                          const std::string_view& s);
  [[nodiscard]] Optional<std::string_view> pack_string_to(PackedBytes* rec,
                                                          const std::string_view& s, Arena* arena);

  // If there is sufficient space, copy `size` bytes from `data` to the beginning of the available
  // region WITHOUT a PackedBytes header, returning a std::string_view of the copied data; if there
  // is insufficient space, set the full flag to true and return None.
  //
  [[nodiscard]] Optional<std::string_view> pack_raw_data(const void* data, usize size);

  template <typename U, typename IntT, typename PackedIntT>
  [[nodiscard]] bool pack_int_impl(U val)
  {
    static_assert(std::is_same_v<std::decay_t<U>, IntT>, "Must be called with exact type");

    PackedIntT* dst = this->pack_record<PackedIntT>();
    if (!dst) {
      return false;
    }
    *dst = val;
    return true;
  }

  template <typename U>
  [[nodiscard]] bool pack_u64(U val)
  {
    return this->pack_int_impl<U, u64, little_u64>(val);
  }

  template <typename U>
  [[nodiscard]] bool pack_u32(U val)
  {
    return this->pack_int_impl<U, u32, little_u32>(val);
  }

  template <typename U>
  [[nodiscard]] bool pack_u16(U val)
  {
    return this->pack_int_impl<U, u16, little_u16>(val);
  }

  template <typename U>
  [[nodiscard]] bool pack_i64(U val)
  {
    return this->pack_int_impl<U, i64, little_i64>(val);
  }

  template <typename U>
  [[nodiscard]] bool pack_i32(U val)
  {
    return this->pack_int_impl<U, i32, little_i32>(val);
  }

  template <typename U>
  [[nodiscard]] bool pack_i16(U val)
  {
    return this->pack_int_impl<U, i16, little_i16>(val);
  }

  template <typename T>
  Optional<ArrayPacker<T>> pack_array()
  {
    PackedArray<T>* array = this->pack_record<PackedArray<T>>();
    if (!array || this->full()) {
      return None;
    }
    array->initialize(0u);
    return ArrayPacker<T>{array, this};
  }

  template <typename T = void, typename Seq,
            typename R = std::conditional_t<std::is_same_v<T, void>, SeqItem<Seq>, T>>
  Optional<ArrayPacker<R>> pack_seq(Seq&& seq)
  {
    if (this->full() || this->space() < packed_array_size<R>(batt::make_copy(seq) | seq::count())) {
      LLFS_DLOG_INFO() << "pack_seq - space check failed";
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

    if (this->full() || this->space() < packed_array_size<T>(item_count)) {
      return None;
    }
    PackedArray<T>* array = this->pack_record<PackedArray<T>>();
    BATT_CHECK_NOT_NULLPTR(array);
    array->initialize(item_count);
    std::copy(first, last, array->begin());
    BATT_CHECK(this->arena_.allocate_front(sizeof(T) * item_count));

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

  u8* pack_varint(u64 n)
  {
    return this->arena_.pack_varint(n);
  }

  Interval<isize> unused() const
  {
    return this->arena_.unused();
  }

  usize buffer_size() const
  {
    return this->buffer_.size();
  }

  MutableBuffer avail_buffer() const
  {
    return MutableBuffer{this->arena_.avail_.begin(), this->arena_.avail_.size()};
  }

  u8* buffer_begin() const
  {
    return static_cast<u8*>(this->buffer_.data());
  }
  u8* buffer_end() const
  {
    return this->buffer_begin() + this->buffer_.size();
  }

  void set_worker_pool(batt::WorkerPool& worker_pool) noexcept
  {
    this->worker_pool_.emplace(worker_pool);
  }

  void clear_worker_pool() noexcept
  {
    this->worker_pool_ = batt::None;
  }

  batt::Optional<batt::WorkerPool&> worker_pool() const noexcept
  {
    return this->worker_pool_;
  }

 private:
  usize estimate_packed_data_size(usize size) const;

  usize estimate_packed_data_size(const PackedBytes& src) const;

  template <typename Policy>
  const void* nocheck_pack_data_to(PackedBytes* rec, const void* data, usize size, Arena* arena,
                                   batt::StaticType<Policy> = {});

  // Copy data into newly "allocated" space at the end of the avail range.
  //
  // Requires that:
  //  - size >= 4
  //  - size <= this->space()
  //
  template <typename Policy>
  const void* nocheck_pack_data_large(PackedBytes* dst, const void* data, usize size, Arena* arena,
                                      batt::StaticType<Policy> = {});

  template <typename Policy>
  const PackedBytes* nocheck_pack_data_copy_to(PackedBytes* dst, const PackedBytes& src,
                                               Arena* arena, batt::StaticType<Policy> = {});

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MutableBuffer buffer_;
  Arena arena_{this, boost::iterator_range<u8*>{this->buffer_begin(), this->buffer_end()}};

  /** \brief Available as a convenience to users of this DataPacker, to speed up the packing of
   * large data using parallelism.
   */
  batt::Optional<batt::WorkerPool&> worker_pool_ = batt::None;
};

}  // namespace llfs

#endif  // LLFS_DATA_PACKER_HPP
