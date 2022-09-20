//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/file_log_driver.hpp>
//
#include <llfs/config.hpp>
#include <llfs/constants.hpp>

#include <batteries/syscall_retry.hpp>

#include <llfs/logging.hpp>

#include <boost/algorithm/string/predicate.hpp>

#include <cctype>
#include <filesystem>
#include <fstream>

namespace llfs {

FileLogDriver::Config FileLogDriver::default_config(const PageCacheOptions& opts)
{
  return Config{
      .min_segment_split_size = 1 * kMiB,
      .max_size = opts.default_log_size(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

StatusOr<std::unique_ptr<FileLogDevice>> FileLogDriver::initialize(
    const Location& location, const Config& config, batt::TaskScheduler& scheduler,
    ConfirmThisWillEraseAllMyData confirm)
{
  initialize_status_codes();

  // Confirm the destructive operation.
  //
  if (confirm != ConfirmThisWillEraseAllMyData::kYes) {
    return ::llfs::make_status(StatusCode::kFileLogEraseNotConfirmed);
  }

  std::error_code ec;

  // Delete the old log device file structure, if present.
  //
  fs::remove_all(location.parent_dir(), ec);
  BATT_REQUIRE_OK(ec) << "Could not remove the FileLogDevice parent directory";

  // Create the parent directory.
  //
  fs::create_directories(location.parent_dir(), ec);
  BATT_REQUIRE_OK(ec) << "Could not create the FileLogDevice parent directory";

  // Write configuration file.
  //
  {
    std::ofstream ofs(location.config_file_path().string().c_str());
    ofs << config.max_size << " " << config.min_segment_split_size;
    if (!ofs.good()) {
      return ::llfs::make_status(::llfs::StatusCode::kFileLogDeviceConfigWriteFailed);
    }
  }

  return FileLogDriver::recover(
      location,
      /*scan_fn=*/
      [](LogDevice::Reader& reader) -> Status {
        BATT_CHECK_EQ(reader.data().size(), 0);
        return OkStatus();
      },
      scheduler);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<FileLogDevice>> FileLogDriver::recover(const Location& location,
                                                                const LogScanFn& scan_fn,
                                                                batt::TaskScheduler& scheduler)
{
  // Read configuration file.
  //
  Config config;
  {
    std::ifstream ifs(location.config_file_path().string().c_str());
    if (ifs.good()) {
      ifs >> config.max_size >> config.min_segment_split_size;
      if (!ifs.good() && !ifs.eof()) {
        return ::llfs::make_status(::llfs::StatusCode::kFileLogDeviceConfigReadFailed);
      }
    }
  }

  // Create the device.
  //
  std::unique_ptr<FileLogDevice> log_device = std::make_unique<FileLogDevice>(
      RingBuffer::TempFile{config.max_size}, location, config, scheduler);

  FileLogDriver& driver = log_device->driver().impl();

  // Read all segment files into the ring buffer.
  //
  StatusOr<ActiveFile> active_file = driver.recover_segments();
  BATT_REQUIRE_OK(active_file);

  // Run the scan function to validate the loaded data and determine the number (if any) of
  // partially flushed bytes that might reside at the end.
  //
  std::unique_ptr<LogDevice::Reader> log_reader =
      log_device->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable);

  StatusOr<slot_offset_type> scan_status = scan_fn(*log_reader);
  BATT_REQUIRE_OK(scan_status);

  // Whatever wasn't consumed by the scan function must be truncated from the end of the log.
  //
  const auto bytes_to_truncate = slot_distance(*scan_status, driver.get_flush_pos());
  auto truncate_status = active_file->truncate(bytes_to_truncate);
  BATT_REQUIRE_OK(truncate_status);

  // Roll back the commit/flush pointers by the specified amount.  If this creates a negative-sized
  // log, panic.
  //
  driver.shared_state_.flush_pos.fetch_sub(bytes_to_truncate);
  driver.shared_state_.commit_pos.fetch_sub(bytes_to_truncate);

  BATT_CHECK(!slot_less_than(driver.shared_state_.flush_pos.get_value(),
                             driver.shared_state_.trim_pos.get_value()))
      << "\n  flush_pos=" << driver.shared_state_.flush_pos.get_value()
      << "\n  trim_pos=" << driver.shared_state_.trim_pos.get_value();

  BATT_CHECK(!slot_less_than(driver.shared_state_.commit_pos.get_value(),
                             driver.shared_state_.trim_pos.get_value()))
      << "\n  commit_pos=" << driver.shared_state_.commit_pos.get_value()
      << "\n  trim_pos=" << driver.shared_state_.trim_pos.get_value();

  // Ready to go!  Start the background tasks (flush and trim).
  //
  driver.start_tasks(std::move(*active_file));

  return log_device;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<FileLogDriver::ActiveFile> FileLogDriver::recover_segments()
{
  LLFS_LOG_INFO() << "FileLogDriver::recover_segments";

  fs::path prefix_dir = this->shared_state_.location.parent_dir();
  std::string prefix_base(FileLogDriver::Location::segment_file_prefix());

  // Scan the directory, picking up all segment files and inserting them into a vector of
  // SegmentFile objects.
  //
  std::vector<SegmentFile> segments;
  std::error_code ec;
  fs::directory_iterator dir_iter(prefix_dir, ec);
  BATT_REQUIRE_OK(ec) << batt::LogLevel::kInfo
                      << "creating dir_iter from prefix_dir: " << prefix_dir;
  for (const auto& p : dir_iter) {
    std::string name = p.path().filename().string();
    LLFS_LOG_INFO() << "found file: " << name;
    if (boost::algorithm::starts_with(name, prefix_base) &&
        boost::algorithm::ends_with(name, FileLogDriver::Location::segment_ext())) {
      Optional<SlotRange> slot_range =
          this->shared_state_.location.slot_range_from_segment_file_name(name);
      if (slot_range) {
        segments.emplace_back(SegmentFile{
            .slot_range = *slot_range,
            .file_name = name,
        });
      }
    }
  }
  LLFS_LOG_INFO() << "finished scanning log segments";

  // Sort the segments by slot offset.
  //
  std::sort(segments.begin(), segments.end(),
            [](const SegmentFile& left, const SegmentFile& right) {
              return left.slot_range.lower_bound < right.slot_range.lower_bound;
            });

  // Detect any discontinuity in the segments; this indicates slot offset integer wrap-around.
  //
  // For example:
  //   [segment_fffe, segment_ffff, segment_0000, segment_0001, segment_0002, ...]
  //                  ^^^^^^^^^^^^^^^^^^^^^^^^^^ We are trying to find pairs like this!
  //
  auto last = std::adjacent_find(
      segments.begin(), segments.end(), [](const SegmentFile& left, const SegmentFile& right) {
        return left.slot_range.upper_bound < right.slot_range.lower_bound;
      });

  if (last != segments.end()) {
    std::rotate(segments.begin(), std::next(last), segments.end());
  }

  const slot_offset_type recovered_lower_bound = [&]() -> slot_offset_type {
    if (segments.empty()) {
      return 0;
    }
    return segments.front().slot_range.lower_bound;
  }();

  LLFS_LOG_INFO() << "recovered log lower_bound=" << recovered_lower_bound;

  // Reset all ring buffer pointers to the start of the recovered segments' slot range.  We will
  // advance flush_pos and commit_pos as we read segment data into the buffer.
  //
  this->shared_state_.trim_pos.set_value(recovered_lower_bound);
  this->shared_state_.flush_pos.set_value(recovered_lower_bound);
  this->shared_state_.commit_pos.set_value(recovered_lower_bound);

  // Read the contents of each segment file into the ring buffer.
  //
  MutableBuffer dst = this->context_.buffer_.get_mut(recovered_lower_bound);
  for (const SegmentFile& s : segments) {
    LLFS_LOG_INFO() << "reading segment file contents: " << s.file_name;
    StatusOr<ConstBuffer> result = s.read(dst);
    BATT_REQUIRE_OK(result);

    dst += result->size();
    this->shared_state_.flush_pos.fetch_add(result->size());
    this->shared_state_.commit_pos.fetch_add(result->size());
  }
  LLFS_LOG_INFO()
      << "finished reading non-head log segments; attempting to re-activate head segment...";

  // Now attempt to open the active (head) segment file.
  //
  StatusOr<ActiveFile> active_file = ActiveFile::open(
      this->shared_state_.location, this->shared_state_.flush_pos.get_value(), dst);

  BATT_REQUIRE_OK(active_file) << batt::LogLevel::kInfo << "opening active log file failed";

  LLFS_LOG_INFO() << "active segment created/recovered; size=" << active_file->size();

  this->shared_state_.flush_pos.fetch_add(active_file->size());
  this->shared_state_.commit_pos.fetch_add(active_file->size());

  // Insert the SegmentFile objects into the segments queue.
  //
  this->shared_state_.segments.push_all(std::move(segments));

  return active_file;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FileLogDriver::start_tasks(ActiveFile&& active_file)
{
  // Start the flush task.
  //
  BATT_CHECK(!this->flush_task_);
  this->flush_task_.emplace(
      this->scheduler_.schedule_task(),
      FlushTaskMain{this->context_.buffer_, this->shared_state_, std::move(active_file)},
      "FileLogDriver::flush_task");

  // Start the trim task.
  //
  BATT_CHECK(!this->trim_task_);
  this->trim_task_.emplace(
      this->scheduler_.schedule_task(),
      [this] {
        this->trim_task_main();
      },
      "FileLogDriver::trim_task");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

FileLogDriver::FileLogDriver(LogStorageDriverContext& context, const Location& location,
                             const Config& config, batt::TaskScheduler& scheduler) noexcept
    : context_{context}
    , scheduler_{scheduler}
    , shared_state_{location, config}
{
}

FileLogDriver::~FileLogDriver() noexcept
{
  this->close().IgnoreError();
}

//----

Status FileLogDriver::set_trim_pos(slot_offset_type trim_pos)
{
  this->shared_state_.trim_pos.set_value(trim_pos);
  return OkStatus();
}

slot_offset_type FileLogDriver::get_trim_pos() const
{
  return this->shared_state_.trim_pos.get_value();
}

StatusOr<slot_offset_type> FileLogDriver::await_trim_pos(slot_offset_type min_offset)
{
  return await_slot_offset(min_offset, this->shared_state_.trim_pos);
}

//----

slot_offset_type FileLogDriver::get_flush_pos() const
{
  return this->shared_state_.flush_pos.get_value();
}

StatusOr<slot_offset_type> FileLogDriver::await_flush_pos(slot_offset_type min_offset)
{
  return await_slot_offset(min_offset, this->shared_state_.flush_pos);
}

//----

Status FileLogDriver::set_commit_pos(slot_offset_type commit_pos)
{
  this->shared_state_.commit_pos.set_value(commit_pos);
  return OkStatus();
}

slot_offset_type FileLogDriver::get_commit_pos() const
{
  return this->shared_state_.commit_pos.get_value();
}

StatusOr<slot_offset_type> FileLogDriver::await_commit_pos(slot_offset_type min_offset)
{
  return await_slot_offset(min_offset, this->shared_state_.commit_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status FileLogDriver::close()
{
  this->shared_state_.halt();

  if (this->trim_task_) {
    this->trim_task_->join();
    this->trim_task_ = None;
  }
  if (this->flush_task_) {
    this->flush_task_->join();
    this->flush_task_ = None;
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FileLogDriver::trim_task_main()
{
  const Status status = [&] {
    // Trim as many segment files as we can, sleeping only when we have to.
    //
    for (;;) {
      // Grab the next finalized segment.
      //
      StatusOr<SegmentFile> oldest_segment = this->shared_state_.segments.await_next();
      BATT_REQUIRE_OK(oldest_segment);

      // Wait for the trim pos to meet or exceed the slot range of this segment.
      //
      {
        auto status =
            await_slot_offset(oldest_segment->slot_range.upper_bound, this->shared_state_.trim_pos);
        BATT_REQUIRE_OK(status);
      }

      // Now we can safely trim the file!
      //
      {
        auto status = oldest_segment->remove();
        BATT_REQUIRE_OK(status);
      }
    }
  }();

  LLFS_LOG_INFO() << "[FileLogDriver::trim_task_main] finished with status=" << status;
}

}  // namespace llfs
