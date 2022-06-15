//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/file_segment_ref.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const FileSegmentRef& file)
{
  return sizeof(PackedFileSegmentRef) + packed_sizeof_str_data(file.path_utf8.length());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedFileSegmentRef* pack_object_to(const FileSegmentRef& from, PackedFileSegmentRef* to,
                                     DataPacker* dst)
{
  if (!dst->pack_string_to(&to->path_utf8, from.path_utf8)) {
    return nullptr;
  }
  to->offset = from.offset;
  to->size = from.size;
  return to;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<FileSegmentRef> unpack_object(const PackedFileSegmentRef& obj, DataReader*)
{
  return FileSegmentRef{
      .path_utf8 = std::string{obj.path_utf8.as_str()},
      .offset = obj.offset,
      .size = obj.size,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const FileSegmentRef& t)
{
  return out << "FileSegmentRef{.path=" << t.path_utf8 << ", .offset=" << t.offset
             << ", .size=" << t.size << ",}";
}

}  // namespace llfs
