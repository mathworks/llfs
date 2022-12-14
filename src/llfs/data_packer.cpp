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

#include <batteries/checked_cast.hpp>
#include <batteries/stream_util.hpp>
#include <batteries/suppress.hpp>

namespace llfs {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer buffer_from(const boost::iterator_range<u8*>& byte_range)
{
  return MutableBuffer{byte_range.begin(), byte_range.size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<u8*> byte_range_from(const MutableBuffer& buffer)
{
  u8* const data = static_cast<u8*>(buffer.data());
  return boost::iterator_range<u8*>{data, data + buffer.size()};
}

}  // namespace

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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class DataPacker::Arena

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ DataPacker::Arena::Arena(DataPacker* packer, boost::iterator_range<u8*> avail) noexcept
    : capacity_{avail.size()}
    , packer_{packer}
    , avail_{avail}
    , full_{false}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DataPacker* DataPacker::Arena::get_packer() const
{
  return this->packer_.get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize DataPacker::Arena::space() const
{
  return this->avail_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize DataPacker::Arena::capacity() const
{
  return this->capacity_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool DataPacker::Arena::full() const
{
  return this->full_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<isize> DataPacker::Arena::unused() const
{
  BATT_CHECK_NOT_NULLPTR(this->packer_.get());

  u8* const buffer_begin = this->packer_->buffer_begin();

  return Interval<isize>{
      this->avail_.begin() - buffer_begin,
      this->avail_.end() - buffer_begin,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void DataPacker::Arena::invalidate()
{
  this->capacity_ = 0;
  this->packer_.reset();
  this->avail_ = boost::iterator_range<u8*>{nullptr, nullptr};
  this->full_ = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void DataPacker::Arena::set_full()
{
  this->full_ = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<DataPacker::Arena> DataPacker::Arena::reserve_front(usize size)
{
  Optional<MutableBuffer> buf = this->allocate_front(size);
  if (!buf) {
    return None;
  }
  return Arena{this->get_packer(), byte_range_from(*buf)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<DataPacker::Arena> DataPacker::Arena::reserve_back(usize size)
{
  Optional<MutableBuffer> buf = this->allocate_back(size);
  if (!buf) {
    return None;
  }
  return Arena{this->get_packer(), byte_range_from(*buf)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<MutableBuffer> DataPacker::Arena::allocate_front(usize size)
{
  if (size > this->space()) {
    this->full_ = true;
    return None;
  }

  return buffer_from(this->nocheck_alloc_front(BATT_CHECKED_CAST(isize, size)));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<MutableBuffer> DataPacker::Arena::allocate_back(usize size)
{
  if (size > this->space()) {
    this->full_ = true;
    return None;
  }

  return buffer_from(this->nocheck_alloc_back(BATT_CHECKED_CAST(isize, size)));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u8* DataPacker::Arena::pack_varint(u64 n)
{
  if (this->full_) {
    return nullptr;
  }

  // Attempt to pack the varint to the front of the available range.
  //
  u8* const dst_begin = this->avail_.begin();
  u8* const avail_end = this->avail_.end();
  u8* dst_end = pack_varint_to(dst_begin, avail_end, n);
  if (dst_end == nullptr) {
    this->full_ = true;
    return nullptr;
  }

  // Commit the packed bytes.
  //
  this->avail_ = boost::iterator_range<u8*>{dst_end, avail_end};

  return dst_end;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<u8*> DataPacker::Arena::nocheck_alloc_front(isize size)
{
  u8* const buf_begin = this->avail_.begin();
  this->avail_.advance_begin(size);
  u8* const buf_end = this->avail_.begin();

  return boost::iterator_range<u8*>{buf_begin, buf_end};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<u8*> DataPacker::Arena::nocheck_alloc_back(isize size)
{
  u8* const buf_end = this->avail_.end();
  this->avail_.advance_end(-size);
  u8* const buf_begin = this->avail_.end();

  return boost::iterator_range<u8*>{buf_begin, buf_end};
}

}  // namespace llfs
