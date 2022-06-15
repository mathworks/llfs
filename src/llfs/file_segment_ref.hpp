//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FILE_SEGMENT_REF_HPP
#define LLFS_FILE_SEGMENT_REF_HPP

#include <llfs/data_layout.hpp>
#include <llfs/data_packer.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>

#include <string>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct FileSegmentRef {
  std::string path_utf8;
  i64 offset;
  i64 size;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedFileSegmentRef {
  PackedBytes path_utf8;
  little_i64 offset;
  little_i64 size;
};

LLFS_DEFINE_PACKED_TYPE_FOR(FileSegmentRef, PackedFileSegmentRef);

usize packed_sizeof(const FileSegmentRef& file);

PackedFileSegmentRef* pack_object_to(const FileSegmentRef& from, PackedFileSegmentRef* to,
                                     DataPacker* dst);

StatusOr<FileSegmentRef> unpack_object(const PackedFileSegmentRef& obj, DataReader*);

std::ostream& operator<<(std::ostream& out, const FileSegmentRef& t);

}  // namespace llfs

#endif  // LLFS_FILE_SEGMENT_REF_HPP
