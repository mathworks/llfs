#include <llfs/data_reader.hpp>
//

#include <turtle/util/slice.hpp>

#include <batteries/stream_util.hpp>

namespace llfs {

Optional<u64> DataReader::read_varint()
{
  if (this->at_end_ || this->bytes_available() == 0) {
    this->at_end_ = true;
    return None;
  }

#ifdef LLFS_VERBOSE_DEBUG_LOGGING
  usize z = 0;
  const u8* p = &this->unread_.front();
#endif  // LLFS_VERBOSE_DEBUG_LOGGING

  u64 n = 0;
  int shift = 0;
  for (;;) {
    const u8 next_byte = this->unread_.front();

#ifdef LLFS_VERBOSE_DEBUG_LOGGING
    z += 1;
#endif  // LLFS_VERBOSE_DEBUG_LOGGING

    this->unread_.pop_front();
    n |= (u64{next_byte} & 0b01111111ull) << shift;
    if (!(next_byte & 0b10000000)) {
      break;
    }
    if (this->bytes_available() == 0) {
      this->at_end_ = true;
      return None;
    }
    shift += 7;
  }

#ifdef LLFS_VERBOSE_DEBUG_LOGGING

  LOG(INFO) << "read_varint([@" << (const void*)p << "; " << z << "]) -> " << std::dec << n
            << " (0x" << std::hex << n << ") " << batt::dump_range(as_slice(p, z));

#endif  // LLFS_VERBOSE_DEBUG_LOGGING

  return n;
}

}  // namespace llfs
