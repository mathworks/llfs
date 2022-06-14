#include <llfs/data_packer.hpp>
//

#include <batteries/stream_util.hpp>

namespace llfs {

DataPacker::DataPacker(const MutableBuffer& buffer) noexcept : buffer_{buffer}
{
}

DataPacker::DataPacker(DataPacker&& that) noexcept
    : buffer_{that.buffer_}
    , avail_{that.avail_}
    , full_{that.full_}
{
  that.invalidate();
}

DataPacker& DataPacker::operator=(DataPacker&& that) noexcept
{
  if (BATT_HINT_TRUE(this != &that)) {
    this->buffer_ = that.buffer_;
    this->avail_ = that.avail_;
    this->full_ = that.full_;
    that.invalidate();
  }
  return *this;
}

void DataPacker::invalidate()
{
  this->buffer_ = MutableBuffer{nullptr, 0};
  this->avail_ = boost::make_iterator_range<u8*>(nullptr, nullptr);
  this->full_ = true;
}

std::size_t DataPacker::estimate_packed_data_size(std::size_t size) const
{
  if (size <= 4) {
    return sizeof(PackedBytes);
  }
  return sizeof(PackedBytes) + size;
}

const void* DataPacker::pack_data(const void* data, std::size_t size)
{
  if (this->full_ || this->space() < this->estimate_packed_data_size(size)) {
    this->full_ = true;
    return nullptr;
  }

  auto* rec = pack_record<PackedBytes>();
  BATT_ASSERT_NOT_NULLPTR(rec);

  return this->nocheck_pack_data_to(rec, data, size);
}

const void* DataPacker::pack_data_to(PackedBytes* rec, const void* data, std::size_t size)
{
  if (this->full_ || this->space() < this->estimate_packed_data_size(size) - sizeof(PackedBytes)) {
    this->full_ = true;
    return nullptr;
  }
  return this->nocheck_pack_data_to(rec, data, size);
}

const void* DataPacker::nocheck_pack_data_to(PackedBytes* rec, const void* data, std::size_t size)
{
  if (size > 4) {
    return this->nocheck_pack_data_large(rec, data, size);
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

const void* DataPacker::nocheck_pack_data_large(PackedBytes* dst, const void* data,
                                                std::size_t size)
{
  u8* before = this->avail_.end();
  this->avail_.advance_end(-std::ptrdiff_t(size));
  u8* packed = this->avail_.end();

  BATT_CHECK_EQ(before - size, packed);
  BATT_CHECK_GE((const void*)packed, (const void*)dst);

  dst->data_offset = packed - reinterpret_cast<const u8*>(dst);
  dst->data_size = size;

  BATT_CHECK_EQ(reinterpret_cast<const u8*>(dst) + dst->data_offset,
                reinterpret_cast<const u8*>(packed));

  std::memcpy(packed, data, size);

  return packed;
}

std::size_t DataPacker::estimate_packed_data_size(const PackedBytes& src) const
{
  if (src.data_offset < sizeof(PackedBytes)) {
    return sizeof(PackedBytes);
  }
  return sizeof(PackedBytes) + src.data_size;
}

const PackedBytes* DataPacker::pack_data_copy(const PackedBytes& src)
{
  if (this->full_ || this->space() < this->estimate_packed_data_size(src)) {
    this->full_ = true;
    return nullptr;
  }

  auto* dst = this->pack_record(batt::StaticType<PackedBytes>{});
  if (!dst) {
    return dst;
  }

  return this->nocheck_pack_data_copy_to(dst, src);
}

const PackedBytes* DataPacker::pack_data_copy_to(PackedBytes* dst, const PackedBytes& src)
{
  if (this->full_ || this->space() < this->estimate_packed_data_size(src)) {
    this->full_ = true;
    return nullptr;
  }

  return this->nocheck_pack_data_copy_to(dst, src);
}

const PackedBytes* DataPacker::nocheck_pack_data_copy_to(PackedBytes* dst, const PackedBytes& src)
{
  // If data offset is within the record itself, then just copy the struct and we're done.
  //
  if (src.data_offset < sizeof(PackedBytes)) {
    // We are doing something that looks unsafe here, but it is really fine.  The point of deleting
    // copy methods on PackedBytes is so you can't do this kind of thing without a big scary pragma
    // like this one.
    //
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif  // __GNUC__

    std::memcpy(dst, &src, sizeof(PackedBytes));

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif  // __GNUC__

  } else {
    (void)this->nocheck_pack_data_large(dst, src.data(), src.data_size);
  }
  return dst;
}

Optional<std::string_view> DataPacker::pack_string(const std::string_view& s)
{
  const void* packed = pack_data(s.data(), s.size());
  if (full_ || packed == nullptr) {
    full_ = true;
    return None;
  }
  return std::string_view(reinterpret_cast<const char*>(packed), s.size());
}

Optional<std::string_view> DataPacker::pack_string_to(PackedBytes* rec, const std::string_view& s)
{
  const void* packed = pack_data_to(rec, s.data(), s.size());
  if (full_ || packed == nullptr) {
    full_ = true;
    return None;
  }
  return std::string_view(reinterpret_cast<const char*>(packed), s.size());
}

u8* DataPacker::pack_varint(u64 n)
{
  const usize bytes_required = packed_sizeof_varint(n);
  if (this->full_ || this->space() < bytes_required) {
    this->full_ = true;
    return nullptr;
  }

  constexpr u64 kLowBitsMask = 0b01111111;
  constexpr u8 kHighBitMask = 0b10000000;

#ifdef LLFS_VERBOSE_DEBUG_LOGGING
  const u64 n_val = n;
#endif  // LLFS_VERBOSE_DEBUG_LOGGING
  u8* p = &this->avail_.front();

  switch (bytes_required) {
    case 10:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 9:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 8:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 7:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 6:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 5:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 4:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 3:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 2:
      avail_.front() = (n & kLowBitsMask) | kHighBitMask;
      avail_.pop_front();
      n >>= 7;
      // fall-through
    case 1:
      avail_.front() = (n & kLowBitsMask);
      avail_.pop_front();
      break;
    default:
      BATT_PANIC() << "bytes_required calculation was wrong! n=" << n
                   << " bytes_required=" << bytes_required;
      break;
  }

#ifdef LLFS_VERBOSE_DEBUG_LOGGING

  LOG(INFO) << "pack_varint(" << std::dec << n_val << " (0x" << std::hex << n_val << std::dec
            << ")) -> [@" << (const void*)p << "; " << std::dec << bytes_required << "] "
            << batt::dump_range(as_slice(p, bytes_required));

#endif  // LLFS_VERBOSE_DEBUG_LOGGING

  return p;
}

}  // namespace llfs
