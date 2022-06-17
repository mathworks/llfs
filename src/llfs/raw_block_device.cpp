//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/raw_block_device.hpp>
//

#include <batteries/hint.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status RawBlockDevice::validate_buffer(const ConstBuffer& buffer, i64 offset)
{
  static constexpr i64 kAlignMask = 512 - 1;

  const bool buffer_alignment_ok = (reinterpret_cast<i64>(buffer.data()) & kAlignMask) == 0;
  const bool buffer_size_ok = (static_cast<i64>(buffer.size()) & kAlignMask) == 0;
  const bool offset_alignment_ok = (offset & kAlignMask) == 0;
  const bool offset_non_negative = offset >= 0;

  if (BATT_HINT_TRUE(buffer_alignment_ok && buffer_size_ok && offset_alignment_ok &&
                     offset_non_negative)) {
    return batt::OkStatus();
  }

  return batt::StatusCode::kInvalidArgument;
}

}  // namespace llfs
