//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_config_file.hpp>
//

#include <llfs/filesystem.hpp>
#include <llfs/seq.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PageArenaFileRef& obj)
{
  return sizeof(PackedPageArenaFileRef) - sizeof(PackedFileSegmentRef) +
         packed_sizeof(obj.arena_file);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedPageArenaFileRef* pack_object_to(const PageArenaFileRef& obj, PackedPageArenaFileRef* packed,
                                       DataPacker* dst)
{
  if (!pack_object_to(obj.arena_file, &packed->arena_file, dst)) {
    return nullptr;
  }
  packed->page_device_id = obj.page_device_id;
  std::memset(packed->reserved_, 0, sizeof(packed->reserved_));
  packed->page_device_uuid = obj.page_device_uuid;

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArenaFileRef> unpack_object(const PackedPageArenaFileRef& packed, DataReader* src)
{
  StatusOr<FileSegmentRef> arena_file = unpack_object(packed.arena_file, src);
  BATT_REQUIRE_OK(arena_file);

  return PageArenaFileRef{
      .arena_file = std::move(*arena_file),
      .page_device_id = packed.page_device_id,
      .page_device_uuid = packed.page_device_uuid,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PageCacheConfigFile& obj)
{
  return sizeof(PackedPageCacheConfigFile) + sizeof(PackedArray<PackedPageArenaFileRef>) +
         (as_seq(obj.arenas)                            //
          | seq::map(BATT_OVERLOADS_OF(packed_sizeof))  //
          | seq::sum());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedPageCacheConfigFile* pack_object_to(const PageCacheConfigFile& obj,
                                          PackedPageCacheConfigFile* packed, DataPacker* dst)
{
  packed->magic = PackedPageCacheConfigFile::kMagic;

  PackedArray<PackedPageArenaFileRef>* packed_arenas =
      dst->pack_record<PackedArray<PackedPageArenaFileRef>>();
  if (!packed_arenas) {
    return nullptr;
  }

  packed_arenas->item_count = 0;
  for (const PageArenaFileRef& arena_ref : obj.arenas) {
    if (!pack_object(arena_ref, dst)) {
      return nullptr;
    }
    packed_arenas->item_count += 1;
  }
  packed->arenas.reset(packed_arenas, dst);

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageCacheConfigFile> unpack_object(const PackedPageCacheConfigFile& packed,
                                            DataReader* src)
{
  if (packed.magic != PackedPageCacheConfigFile::kMagic) {
    return {batt::StatusCode::kInternal};
  }

  PageCacheConfigFile obj;
  for (const PackedPageArenaFileRef& packed_arena : *packed.arenas) {
    StatusOr<PageArenaFileRef> arena = unpack_object(packed_arena, src);
    BATT_REQUIRE_OK(arena);
    obj.arenas.emplace_back(std::move(*arena));
  }

  return {std::move(obj)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageCacheConfigFile> read_page_cache_config_file(std::string_view filename)
{
  // Open the cache config file and close it when we are done.
  //
  StatusOr<int> fd = open_file_read_only(filename);
  BATT_REQUIRE_OK(fd);

  const auto close_fd = batt::finally([fd] {
    ::close(*fd);
  });

  // Get the file size and allocate a buffer to hold the current config.
  //
  const StatusOr<i64> file_size = sizeof_fd(*fd);
  BATT_REQUIRE_OK(file_size);

  std::unique_ptr<u8[]> buffer{new u8[*file_size]};

  // Read the entire contents of the file.
  //
  const StatusOr<ConstBuffer> data =
      read_fd(*fd, MutableBuffer{buffer.get(), static_cast<usize>(*file_size)}, /*offset=*/0);

  BATT_REQUIRE_OK(data);

  // Parse the structured data.
  //
  DataReader reader{*data};

  StatusOr<PageCacheConfigFile> cache_config =
      read_object(&reader, batt::StaticType<PageCacheConfigFile>{});

  return cache_config;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status write_page_cache_config_file(std::string_view filename,
                                    const PageCacheConfigFile& cache_config)
{
  // Open the cache config file and close it when we are done.
  //
  StatusOr<int> fd = open_file_read_write(filename, OpenForAppend{false});
  BATT_REQUIRE_OK(fd);

  const auto close_fd = batt::finally([fd] {
    ::close(*fd);
  });

  // Allocate a buffer to hold the new packed cache config.
  //
  const usize required_size = packed_sizeof(cache_config);
  std::unique_ptr<u8[]> buffer{new u8[required_size]};

  DataPacker packer{MutableBuffer{buffer.get(), required_size}};

  BATT_CHECK_NOT_NULLPTR(pack_object(cache_config, &packer));
  BATT_REQUIRE_OK(write_fd(*fd, ConstBuffer{buffer.get(), required_size}, /*offset=*/0));
  BATT_REQUIRE_OK(truncate_fd(*fd, required_size));
  BATT_CHECK_EQ(BATT_OK_RESULT_OR_PANIC(sizeof_fd(*fd)), static_cast<i64>(required_size));

  return OkStatus();
}

}  // namespace llfs
