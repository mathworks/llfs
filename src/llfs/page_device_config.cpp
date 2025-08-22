//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_device_config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/config.hpp>
#include <llfs/ioring_page_file_device.hpp>
#include <llfs/page_layout.hpp>

#include <batteries/stream_util.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_PRINT_OBJECT_IMPL(PackedPageDeviceConfig,  //
                       (page_0_offset)          //
                       (device_id)              //
                       (page_count)             //
                       (page_size_log2)         //
                       (uuid)                   //
)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status configure_storage_object(StorageFileBuilder::Transaction& txn,
                                FileOffsetPtr<PackedPageDeviceConfig&> p_config,
                                const PageDeviceConfigOptions& options)
{
  // If the PageDevice is the last in the file, then the max page count needs to be greater or equal
  // to initial page count. If the PageDevice is not marked last in file, the page counts need to be
  // equal.
  //
  const bool last_in_file = options.last_in_file.value_or(false);
  const PageCount max_page_count = options.max_page_count.value_or(options.page_count);

  BATT_CHECK((last_in_file && max_page_count >= options.page_count) ||
             (!last_in_file && max_page_count == options.page_count))
      << "Error: Invalid PageDevice Configuration. Incorrect mix of last_in_file, page_count, and "
         "max_page_count options";

  const i64 page_size = (i64{1} << options.page_size_log2);
  const i64 pages_total_size = page_size * options.page_count;

  const Interval<i64> pages_offset = txn.reserve_aligned(options.page_size_log2, pages_total_size);

  p_config.absolute_page_0_offset(pages_offset.lower_bound);
  if (!options.device_id) {
    p_config->device_id = txn.reserve_device_id();
  } else {
    p_config->device_id = *options.device_id;
  }

  p_config->page_count = max_page_count;
  p_config->page_size_log2 = options.page_size_log2;
  p_config->uuid = options.uuid.value_or(boost::uuids::random_generator{}());
  p_config->set_last_in_file(last_in_file);

  txn.require_pre_flush_action([pages_offset, page_size = page_size,
                                page_count = options.page_count](RawBlockFile& file) -> Status {
    LLFS_DVLOG(1) << "truncating to " << BATT_INSPECT(pages_offset.upper_bound)
                  << BATT_INSPECT(page_size) << BATT_INSPECT(page_count);
    Status truncate_status = file.truncate_at_least(pages_offset.upper_bound);
    BATT_REQUIRE_OK(truncate_status);

    if (!kFastIoRingPageDeviceInit) {
      // Initialize all the packed page headers.
      //
      std::aligned_storage_t<kDirectIOBlockSize, kDirectIOBlockAlign> buffer;
      std::memset(&buffer, 0, sizeof(buffer));

      auto& page_header = reinterpret_cast<PackedPageHeader&>(buffer);
      page_header.magic = PackedPageHeader::kMagic;
      page_header.crc32 = PackedPageHeader::kCrc32NotSet;
      page_header.page_id = PackedPageId{.id_val = kInvalidPageId};

      for (u64 page_i = 0; page_i < page_count; ++page_i) {
        const i64 page_offset = pages_offset.lower_bound + page_i * page_size;
        LLFS_DVLOG(1) << "writing null page header | " << BATT_INSPECT(page_i)
                      << BATT_INSPECT(page_offset);
        Status status = write_all(file, page_offset, ConstBuffer{&buffer, sizeof(buffer)});
        BATT_REQUIRE_OK(status);
      }
    }

    return OkStatus();
  });

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<PageDevice>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& /*storage_context*/, const std::string& file_name,
    const FileOffsetPtr<const PackedPageDeviceConfig&>& p_config,
    const IoRingFileRuntimeOptions& file_options)
{
  StatusOr<IoRing::File> file = open_ioring_file(file_name, file_options);
  BATT_REQUIRE_OK(file);

  return std::make_unique<IoRingPageFileDevice>(std::move(*file), p_config);
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
