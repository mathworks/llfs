#include <llfs/crc.hpp>

namespace llfs {

boost::crc_basic<64> make_crc64()
{
  return boost::crc_basic<64>(0x42F0E1EBA9EA3693, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, false,
                              false);
}

}  // namespace llfs
