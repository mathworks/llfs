//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/data_packer.hpp>
//

#include <llfs/varint.hpp>

#include <batteries/algo/parallel_copy.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/stream_util.hpp>
#include <batteries/suppress.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DataPacker::DataPacker(const MutableBuffer& buffer) noexcept : buffer_{buffer}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DataPacker::DataPacker(DataPacker&& that) noexcept
    : buffer_{that.buffer_}
    , arena_{this, that.arena_.avail_}
{
  that.invalidate();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DataPacker& DataPacker::operator=(DataPacker&& that) noexcept
{
  if (BATT_HINT_TRUE(this != &that)) {
    this->buffer_ = that.buffer_;
    this->arena_ = std::move(that.arena_);
    that.invalidate();
  }
  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void DataPacker::invalidate()
{
  this->buffer_ = MutableBuffer{nullptr, 0};
  this->arena_.invalidate();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data(const void* data, usize size)
{
  if (this->full() || this->space() < this->estimate_packed_data_size(size)) {
    this->set_full();
    return nullptr;
  }

  auto* rec = this->pack_record<PackedBytes>();
  BATT_ASSERT_NOT_NULLPTR(rec);

  return this->nocheck_pack_data_to(rec, data, size, &this->arena_,
                                    batt::StaticType<AllocBackPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data(const void* data, usize size, Arena* arena)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_data(data, size);
  }

  if (this->full() || arena->full() || this->space() < sizeof(PackedBytes) ||
      arena->space() < (this->estimate_packed_data_size(size) - sizeof(PackedBytes))) {
    this->set_full();
    arena->set_full();
    return nullptr;
  }

  auto* rec = this->pack_record<PackedBytes>();
  BATT_ASSERT_NOT_NULLPTR(rec);

  return this->nocheck_pack_data_to(rec, data, size, arena, batt::StaticType<AllocFrontPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data(const void* data, usize size, UseParallelCopy use_parallel_copy)
{
  if (this->full() || this->space() < this->estimate_packed_data_size(size)) {
    this->set_full();
    return nullptr;
  }

  auto* rec = this->pack_record<PackedBytes>();
  BATT_ASSERT_NOT_NULLPTR(rec);

  return this->nocheck_pack_data_to(rec, data, size, &this->arena_,
                                    batt::StaticType<AllocBackPolicy>{}, use_parallel_copy);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data(const void* data, usize size, Arena* arena,
                                  UseParallelCopy use_parallel_copy)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_data(data, size);
  }

  if (this->full() || arena->full() || this->space() < sizeof(PackedBytes) ||
      arena->space() < (this->estimate_packed_data_size(size) - sizeof(PackedBytes))) {
    this->set_full();
    arena->set_full();
    return nullptr;
  }

  auto* rec = this->pack_record<PackedBytes>();
  BATT_ASSERT_NOT_NULLPTR(rec);

  return this->nocheck_pack_data_to(rec, data, size, arena, batt::StaticType<AllocFrontPolicy>{},
                                    use_parallel_copy);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data_to(PackedBytes* rec, const void* data, usize size)
{
  if (this->full() || this->space() < this->estimate_packed_data_size(size) - sizeof(PackedBytes)) {
    this->set_full();
    return nullptr;
  }
  return this->nocheck_pack_data_to(rec, data, size, &this->arena_,
                                    batt::StaticType<AllocBackPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data_to(PackedBytes* rec, const void* data, usize size, Arena* arena)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_data_to(rec, data, size);
  }

  if (arena->full() ||
      arena->space() < (this->estimate_packed_data_size(size) - sizeof(PackedBytes))) {
    arena->set_full();
    this->set_full();
    return nullptr;
  }
  return this->nocheck_pack_data_to(rec, data, size, arena, batt::StaticType<AllocFrontPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data_to(PackedBytes* rec, const void* data, usize size,
                                     UseParallelCopy use_parallel_copy)
{
  if (this->full() || this->space() < this->estimate_packed_data_size(size) - sizeof(PackedBytes)) {
    this->set_full();
    return nullptr;
  }
  return this->nocheck_pack_data_to(rec, data, size, &this->arena_,
                                    batt::StaticType<AllocBackPolicy>{}, use_parallel_copy);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const void* DataPacker::pack_data_to(PackedBytes* rec, const void* data, usize size, Arena* arena,
                                     UseParallelCopy use_parallel_copy)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_data_to(rec, data, size);
  }

  if (arena->full() ||
      arena->space() < (this->estimate_packed_data_size(size) - sizeof(PackedBytes))) {
    arena->set_full();
    this->set_full();
    return nullptr;
  }
  return this->nocheck_pack_data_to(rec, data, size, arena, batt::StaticType<AllocFrontPolicy>{},
                                    use_parallel_copy);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBytes* DataPacker::pack_data_copy(const PackedBytes& src)
{
  if (this->full() || this->space() < this->estimate_packed_data_size(src)) {
    this->set_full();
    return nullptr;
  }

  auto* dst = this->pack_record(batt::StaticType<PackedBytes>{});
  if (!dst) {
    return dst;
  }

  return this->nocheck_pack_data_copy_to(dst, src, &this->arena_,
                                         batt::StaticType<AllocBackPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBytes* DataPacker::pack_data_copy(const PackedBytes& src, Arena* arena)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_data_copy(src);
  }

  if (this->full() || arena->full() || this->space() < sizeof(PackedBytes) ||
      arena->space() < (this->estimate_packed_data_size(src) - sizeof(PackedBytes))) {
    this->set_full();
    arena->set_full();
    return nullptr;
  }

  auto* dst = this->pack_record(batt::StaticType<PackedBytes>{});
  if (!dst) {
    return dst;
  }

  return this->nocheck_pack_data_copy_to(dst, src, arena, batt::StaticType<AllocFrontPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBytes* DataPacker::pack_data_copy_to(PackedBytes* dst, const PackedBytes& src)
{
  if (this->full() || this->space() < this->estimate_packed_data_size(src)) {
    this->set_full();
    return nullptr;
  }

  return this->nocheck_pack_data_copy_to(dst, src, &this->arena_,
                                         batt::StaticType<AllocBackPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedBytes* DataPacker::pack_data_copy_to(PackedBytes* dst, const PackedBytes& src,
                                                 Arena* arena)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_data_copy_to(dst, src);
  }

  if (arena->full() ||
      arena->space() < (this->estimate_packed_data_size(src) - sizeof(PackedBytes))) {
    this->set_full();
    arena->set_full();
    return nullptr;
  }

  return this->nocheck_pack_data_copy_to(dst, src, arena, batt::StaticType<AllocFrontPolicy>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<std::string_view> DataPacker::pack_string(const std::string_view& s)
{
  const void* packed = this->pack_data(s.data(), s.size());
  if (this->full() || packed == nullptr) {
    this->set_full();
    return None;
  }
  return std::string_view(static_cast<const char*>(packed), s.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<std::string_view> DataPacker::pack_string(const std::string_view& s, Arena* arena)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_string(s);
  }
  const void* packed = this->pack_data(s.data(), s.size(), arena);
  if (this->full() || packed == nullptr) {
    this->set_full();
    return None;
  }
  return std::string_view(static_cast<const char*>(packed), s.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<std::string_view> DataPacker::pack_string_to(PackedBytes* rec, const std::string_view& s)
{
  const void* packed = this->pack_data_to(rec, s.data(), s.size());
  if (this->full() || packed == nullptr) {
    this->set_full();
    return None;
  }
  return std::string_view(static_cast<const char*>(packed), s.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<std::string_view> DataPacker::pack_string_to(PackedBytes* rec, const std::string_view& s,
                                                      Arena* arena)
{
  if (BATT_HINT_FALSE(arena == &this->arena_)) {
    return this->pack_string_to(rec, s);
  }
  const void* packed = this->pack_data_to(rec, s.data(), s.size(), arena);
  if (this->full() || packed == nullptr) {
    this->set_full();
    return None;
  }
  return std::string_view(static_cast<const char*>(packed), s.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<std::string_view> DataPacker::pack_raw_data(const void* data, usize size)
{
  Optional<MutableBuffer> buf = this->arena_.allocate_front(size);
  if (!buf) {
    return None;
  }
  std::memcpy(buf->data(), data, size);

  return std::string_view{static_cast<const char*>(buf->data()), buf->size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<std::string_view> DataPacker::pack_raw_data(const void* data, usize size,
                                                     UseParallelCopy use_parallel_copy)
{
  if (!use_parallel_copy || !this->worker_pool_) {
    return this->pack_raw_data(data, size);
  }

  Optional<MutableBuffer> buf = this->arena_.allocate_front(size);
  if (!buf) {
    return None;
  }

  u8* const dst_begin = static_cast<u8*>(buf->data());
  {
    const batt::TaskCount max_tasks{this->worker_pool_->size() + 1};
    const batt::TaskSize min_task_size{DataPacker::min_parallel_copy_size()};

    batt::ScopedWorkContext work_context{*this->worker_pool_};

    const u8* src_begin = reinterpret_cast<const u8*>(data);
    const u8* src_end = src_begin + size;

    batt::parallel_copy(work_context, src_begin, src_end, dst_begin, min_task_size, max_tasks);
  }

  return std::string_view{static_cast<const char*>(buf->data()), buf->size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<DataPacker::Arena> DataPacker::reserve_arena(usize size)
{
  return this->arena_.reserve_back(size);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize DataPacker::estimate_packed_data_size(usize size) const
{
  if (size <= 4) {
    return sizeof(PackedBytes);
  }
  return sizeof(PackedBytes) + size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize DataPacker::estimate_packed_data_size(const PackedBytes& src) const
{
  if (src.data_offset < sizeof(PackedBytes)) {
    return sizeof(PackedBytes);
  }
  return sizeof(PackedBytes) + src.data_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Policy>
const void* DataPacker::nocheck_pack_data_to(PackedBytes* dst, const void* data, usize size,
                                             Arena* arena, batt::StaticType<Policy> policy,
                                             UseParallelCopy use_parallel_copy)
{
  if (!use_parallel_copy || !this->worker_pool_ || size <= 4) {
    return this->nocheck_pack_data_to(dst, data, size, arena, policy);
  }

  boost::iterator_range<u8*> buf = Policy::nocheck_alloc(arena, static_cast<isize>(size));

  u8* const before = buf.end();
  u8* const packed = buf.begin();

  BATT_CHECK_EQ(before - size, packed);
  BATT_CHECK_GE((const void*)packed, (const void*)dst);

  dst->data_offset = packed - reinterpret_cast<const u8*>(dst);
  dst->data_size = size;

  BATT_CHECK_EQ(reinterpret_cast<const u8*>(dst) + dst->data_offset, const_cast<const u8*>(packed));
  {
    const batt::TaskCount max_tasks{this->worker_pool_->size() + 1};
    const batt::TaskSize min_task_size{DataPacker::min_parallel_copy_size()};

    batt::ScopedWorkContext work_context{*this->worker_pool_};

    const u8* src_begin = reinterpret_cast<const u8*>(data);
    const u8* src_end = src_begin + size;

    batt::parallel_copy(work_context, src_begin, src_end, packed, min_task_size, max_tasks);
  }
  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Policy>
const void* DataPacker::nocheck_pack_data_to(PackedBytes* rec, const void* data, usize size,
                                             Arena* arena, batt::StaticType<Policy> policy)
{
  if (size > 4) {
    return this->nocheck_pack_data_large(rec, data, size, arena, policy);
  }

  rec->data_offset = sizeof(PackedBytes) - size;
  BATT_CHECK_GE(rec->data_offset.value(), 4);
  BATT_CHECK_LE(rec->data_offset.value(), sizeof(PackedBytes));

  u8* const packed = ((u8*)rec) + rec->data_offset;
  if (size == 0) {
    rec->data_size = 0;
  } else {
    std::memcpy(packed, data, size);
  }

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Policy>
const void* DataPacker::nocheck_pack_data_large(PackedBytes* dst, const void* data, usize size,
                                                Arena* arena, batt::StaticType<Policy>)
{
  // Data buffer is being allocated here for the data part (could be the data part of key or value
  // for instance)
  boost::iterator_range<u8*> buf = Policy::nocheck_alloc(arena, static_cast<isize>(size));

  u8* const before = buf.end();
  u8* const packed = buf.begin();

  BATT_CHECK_EQ(before - size, packed);
  BATT_CHECK_GE((const void*)packed, (const void*)dst);

  dst->data_offset = packed - reinterpret_cast<const u8*>(dst);
  dst->data_size = size;

  BATT_CHECK_EQ(reinterpret_cast<const u8*>(dst) + dst->data_offset, const_cast<const u8*>(packed));

  std::memcpy(packed, data, size);

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Policy>
const PackedBytes* DataPacker::nocheck_pack_data_copy_to(PackedBytes* dst, const PackedBytes& src,
                                                         Arena* arena,
                                                         batt::StaticType<Policy> policy)
{
  // If data offset is within the record itself, then just copy the struct and we're done.
  //
  if (src.data_offset < sizeof(PackedBytes)) {
    // We are doing something that looks unsafe here, but it is really fine.  The point of deleting
    // copy methods on PackedBytes is so you can't do this kind of thing without a big scary pragma
    // like this one.
    //
    BATT_SUPPRESS_IF_GCC("-Wclass-memaccess")
    std::memcpy(dst, &src, sizeof(PackedBytes));
    BATT_UNSUPPRESS_IF_GCC()

  } else {
    const void* packed_data =
        this->nocheck_pack_data_large(dst, src.data(), src.data_size, arena, policy);

    BATT_CHECK_EQ(dst->data(), packed_data);
  }
  return dst;
}

}  // namespace llfs
