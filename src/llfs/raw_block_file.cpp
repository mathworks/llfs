//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/raw_block_file.hpp>
//

#include <batteries/hint.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status RawBlockFile::validate_buffer(const ConstBuffer& buffer, i64 offset)
{
  static constexpr i64 kAlignMask = kDirectIOBlockAlign - 1;

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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ i64 RawBlockFile::align_up(i64 n)
{
  return (n + 511) & ~511ll;
}

// Return the greatest block-aligned value not greater than n.
//
/*static*/ i64 RawBlockFile::align_down(i64 n)
{
  return n & ~511ll;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status write_all(RawBlockFile& file, i64 offset, const ConstBuffer& data)
{
  return transfer_all(offset, data, [&file](i64 offset, const ConstBuffer& data) {
    return file.write_some(offset, data);
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status read_all(RawBlockFile& file, i64 offset, const MutableBuffer& buffer)
{
  return transfer_all(offset, buffer, [&file](i64 offset, const MutableBuffer& buffer) {
    return file.read_some(offset, buffer);
  });
}

}  // namespace llfs
