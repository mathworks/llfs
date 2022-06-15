//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_arena_file.hpp>
//

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/crc.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/ioring_log_device.hpp>
#include <llfs/ioring_page_file_device.hpp>
#include <llfs/ring_buffer.hpp>
#include <llfs/status.hpp>
#include <llfs/system_config.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/math.hpp>

#include <boost/uuid/random_generator.hpp>

#include <cstdlib>

#include <unistd.h>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto driver_config_from_arena_config(const FileOffsetPtr<PackedPageArenaConfig>& config)
{
  return;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> open_page_device_file_impl(batt::TaskScheduler& scheduler, IoRing& ioring,
                                               const FileSegmentRef& file_ref,
                                               const PageArenaFileRuntimeOptions& options,
                                               PackedPageArenaConfig* config, bool write_config,
                                               ConfirmThisWillEraseAllMyData confirm)
{
  return {batt::StatusCode::kUnimplemented};
#if 0
  int flags = O_DIRECT | O_SYNC;
  if (options.read_only == ReadOnly::kTrue) {
    flags |= O_RDONLY;
  } else {
    flags |= O_RDWR;
  }

  const int fd = open(file_ref.path_utf8.c_str(), flags);
  if (fd < 0) {
    return batt::status_from_retval(fd);
  }

  int fd2 = dup(fd);
  if (fd2 < 0) {
    close(fd);
    return batt::status_from_retval(fd2);
  }
  const auto close_fd2 = batt::finally([&fd2] {
    if (fd2 != -1) {
      ::close(fd2);
      fd2 = -1;
    }
  });

  IoRing::File file{ioring, fd};

  FileOffsetPtr<PackedPageArenaConfig> packed_config;

  // Set the packed config location to the end of the file segment.
  //
  packed_config.file_offset =
      batt::checked_cast<i64>(file_ref.offset + file_ref.size) - sizeof(PackedPageArenaConfig);

  // Sanity check: verify that the file is large enough.
  {
    const StatusOr<i64> file_size = sizeof_fd(fd);
    BATT_REQUIRE_OK(file_size);

    BATT_CHECK_LE(packed_config.file_offset + sizeof(PackedPageArenaConfig),
                  batt::checked_cast<usize>(*file_size));
  }

  // Copy config from input or read from disk.
  //
  if (!write_config) {
    Status read_status = packed_config.read_from_ioring_file(file);
    BATT_REQUIRE_OK(read_status);

    if (packed_config->magic != PackedPageArenaConfig::kMagic) {
      // TODO [tastolfi 2022-02-08] custom code: No arena config found at this location.
      //
      return {batt::StatusCode::kDataLoss};
    }

    if (packed_config->crc64 != get_crc64(*packed_config)) {
      return {batt::StatusCode::kDataLoss};
    }
  } else {
    BATT_CHECK_NOT_NULLPTR(config);
    *packed_config = *config;
  }
  //
  // At this point `*packed_config` is valid.

  const auto page_allocator_log_config = IoRingLogDriver::Config::from_packed(
      packed_config.get_page_allocator().get_log_device().decay_copy());

  // Write the config to disk if necessary.
  //
  if (write_config) {
    Status allocator_log_init =
        initialize_ioring_log_device(file, page_allocator_log_config, confirm);

    BATT_REQUIRE_OK(allocator_log_init);

    Status write_status = packed_config.write_to_ioring_file(file);

    BATT_REQUIRE_OK(write_status) << batt::LogLevel::kError << "failed to write config block; "
                                  << BATT_INSPECT(packed_config)
                                  << " size=" << buffer_from_struct(*config).size();
  }

  BATT_CHECK_EQ(file_ref.size, packed_config->total_arena_size()) << packed_config;

  IoRingLogDriver::Options page_allocator_log_options;
  page_allocator_log_options.name = options.name;
  page_allocator_log_options.queue_depth_log2 = batt::log2_ceil(options.log_queue_depth);

  IoRingLogDeviceFactory page_allocator_log_factory{fd2, page_allocator_log_config,
                                                    page_allocator_log_options};
  fd2 = -1;

  StatusOr<std::unique_ptr<PageAllocator>> page_allocator = PageAllocator::recover(
      scheduler, options.name,
      PageIdFactory{config->page_device.page_count, config->page_device.device_id},
      page_allocator_log_factory);
  BATT_REQUIRE_OK(page_allocator);

  return PageArena{std::make_unique<IoRingPageFileDevice>(
                       std::move(file), packed_config.get_page_device().decay_copy()),
                   std::move(*page_allocator)};
#endif
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> page_arena_from_file(batt::TaskScheduler& scheduler, IoRing& ioring,
                                         const FileSegmentRef& file_ref,
                                         const PageArenaFileRuntimeOptions& options,
                                         PackedPageArenaConfig* config_out)
{
  return open_page_device_file_impl(scheduler, ioring, file_ref, options, config_out,
                                    /*write_config=*/false, ConfirmThisWillEraseAllMyData::kNo);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageArena> initialize_page_arena_file(batt::TaskScheduler& scheduler, IoRing& ioring,
                                               const FileSegmentRef& file_ref,
                                               const PageArenaFileRuntimeOptions& options,
                                               const PackedPageArenaConfig& config,
                                               ConfirmThisWillEraseAllMyData confirm)
{
  if (confirm != ConfirmThisWillEraseAllMyData::kYes) {
    return Status{batt::StatusCode::kInvalidArgument};  // TODO [tastolfi 2021-10-20] "operation not
                                                        // confirmed!"
  }

  //  auto config_copy = config;
  return open_page_device_file_impl(scheduler, ioring, file_ref, options, /*&config_copy*/ nullptr,
                                    /*write_config=*/true, confirm);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::vector<std::unique_ptr<PageArenaConfigInFile>>> read_arena_configs_from_file(
    const std::string& filename)
{
  return {batt::StatusCode::kUnimplemented};

#if 0
    std::vector<std::unique_ptr<PageArenaConfigInFile>> configs;

  StatusOr<int> fd = open_file_read_only(filename);
  BATT_CHECK_OK(fd);

  auto close_fd = batt::finally([&] {
    ::close(*fd);
  });

  i64 file_size = ::lseek(*fd, 0, SEEK_END);
  BATT_CHECK_OK(batt::status_from_retval(file_size));

  if (file_size < batt::checked_cast<i64>(sizeof(PackedPageArenaConfig))) {
    return configs;
  }

  i64 arena_config_pos = file_size - sizeof(PackedPageArenaConfig);
  while (arena_config_pos >= 0) {
    VLOG(1) << BATT_INSPECT(arena_config_pos);

    auto record = std::make_unique<PageArenaConfigInFile>();
    PackedPageArenaConfig& config = record->config;
    std::memset(&config, 0, sizeof(PackedPageArenaConfig));

    StatusOr<ConstBuffer> data = read_fd(*fd, mutable_buffer_from_struct(config), arena_config_pos);
    BATT_CHECK_OK(data);

    if (config.magic != PackedPageArenaConfig::kMagic) {
      return configs;
    }

    if (config.crc64 != get_crc64(config)) {
      return {batt::StatusCode::kDataLoss};
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Now that we believe the data integrity is intact, we can make hard assertions about
    // invariants.
    //
    const i64 device_config_pos = arena_config_pos + offsetof(PackedPageArenaConfig, page_device);

    const i64 device_page_0_pos = device_config_pos + config.page_device.page_0_offset;

    const i64 allocator_config_pos =
        arena_config_pos + offsetof(PackedPageArenaConfig, page_allocator);

    const i64 allocator_log_config_pos =
        allocator_config_pos + offsetof(PackedPageAllocatorConfig, log_device);

    const i64 allocator_log_pos =
        allocator_log_config_pos + config.page_allocator.log_device.block_0_offset;

    BATT_CHECK_EQ(round_up_to_page_size_multiple(config.page_device.page_count *
                                                     (1ll << config.page_device.page_size_log2) +
                                                 batt::checked_cast<u64>(device_page_0_pos)),
                  batt::checked_cast<u64>(allocator_log_pos));

    BATT_CHECK_EQ(batt::round_up_bits(
                      12, batt::checked_cast<u64>(allocator_log_pos +
                                                  config.page_allocator.log_device.physical_size)),
                  batt::checked_cast<u64>(arena_config_pos));

    BATT_CHECK_EQ(config.end_offset, sizeof(PackedPageArenaConfig));

    BATT_CHECK_GE(arena_config_pos + config.begin_offset, 0);
    //+++++++++++-+-+--+----- --- -- -  -  -   -

    record->file_ref.path_utf8 = filename;
    record->file_ref.offset = arena_config_pos + config.begin_offset;
    record->file_ref.size = config.end_offset - config.begin_offset;

    configs.emplace_back(std::move(record));

    arena_config_pos += config.begin_offset;
    arena_config_pos -= sizeof(PackedPageArenaConfig);
  }

  return configs;
#endif
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
