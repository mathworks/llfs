//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FILE_LOG_DRIVER_HPP
#define LLFS_FILE_LOG_DRIVER_HPP

#include <llfs/basic_log_storage_driver.hpp>
#include <llfs/basic_ring_buffer_log_device.hpp>
#include <llfs/confirm.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/interval.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_cache_options.hpp>
#include <llfs/slot.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/queue.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/async/watch.hpp>

#include <cstddef>
#include <deque>
#include <functional>
#include <string>

namespace llfs {

class FileLogDriver;

using FileLogDevice = BasicRingBufferLogDevice<FileLogDriver>;

// Buffered log driver that flushes log data to a series of "segment" files.
//
class FileLogDriver
{
 public:
  // See <llfs/file_log_driver/location.cpp>
  //
  class Location
  {
   public:
    static std::string_view segment_ext()
    {
      return ".tdblog";
    }

    static std::string_view segment_file_prefix()
    {
      return "segment_";
    }

    static std::string_view active_segment_file_name()
    {
      return "head.tdblog";
    }

    static std::string_view config_file_name()
    {
      return "log.tdb_config";
    }

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

    Location() = default;

    template <typename... Args, typename = batt::EnableIfNoShadow<Location, Args...>>
    explicit Location(Args&&... args) noexcept : parent_dir_(BATT_FORWARD(args)...)
    {
    }

    const fs::path& parent_dir() const
    {
      return this->parent_dir_;
    }

    fs::path config_file_path() const;

    Optional<SlotRange> slot_range_from_segment_file_name(const std::string& name) const;

    std::string segment_file_name_from_slot_range(const SlotRange& slot_range) const;

    fs::path active_segment_file_path() const;

   private:
    fs::path parent_dir_;
  };

  // See <llfs/file_log_driver/config.cpp>
  //
  struct Config {
    // The size at (or above) which a segment file is closed and a new one created.
    //
    // This parameter trades off between the cost of writing data to the log and the upper bound on
    // wasted log space due to unprocessed trim operations.  Trimming the log eventually results in
    // the removal of segment files, but only once none of the contents of a segment file could be
    // in use.  Conversely, the cost of opening new segment files and closing old ones is amortized
    // over all the writes contained by a single segment file; the bigger the file, the lower the
    // amortized cost of managing segments.
    //
    std::size_t min_segment_split_size;

    // The maximum capacity of the log; i.e., the maximum allowed distance in bytes from trim offset
    // to commit offset.
    //
    std::size_t max_size;

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  };

  // See <llfs/file_log_driver/active_file.hpp>
  //
  class ActiveFile;

  // See <llfs/file_log_driver/segment_file.cpp>
  //
  struct SegmentFile {
    // Permanently delete the file from durable storage.
    //
    Status remove();

    // Read the contents of this file into memory.  On success, returns the prefix of `buffer` that
    // was filled with data from the segment file.
    //
    StatusOr<ConstBuffer> read(MutableBuffer buffer) const;

    SlotRange slot_range;
    std::string file_name;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static Config default_config(const PageCacheOptions& opts);

  static StatusOr<std::unique_ptr<FileLogDevice>> initialize(const Location& location,
                                                             const Config& config,
                                                             batt::TaskScheduler& scheduler,
                                                             ConfirmThisWillEraseAllMyData confirm);

  // Recover a closed or crashed log by loading whatever is possible into memory, creating a
  // LogDevice::Reader to access it, and executing `scan_fn` to validate the data.  `scan_fn` should
  // consume as much as possible; any data left unconsumed when it returns will be truncated before
  // returning the final device.  If the scan_fn returns a non-ok status, that is returned in place
  // of the recovered device.
  //
  static StatusOr<std::unique_ptr<FileLogDevice>> recover(const Location& location,
                                                          const LogScanFn& scan_fn,
                                                          batt::TaskScheduler& scheduler);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit FileLogDriver(LogStorageDriverContext& context, const Location& location,
                         const Config& config, batt::TaskScheduler& scheduler) noexcept;

  ~FileLogDriver() noexcept;

  FileLogDriver(const FileLogDriver&) = delete;
  FileLogDriver& operator=(const FileLogDriver&) = delete;

  //----

  Status set_trim_pos(slot_offset_type trim_pos);

  slot_offset_type get_trim_pos() const;

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type min_offset);

  //----

  slot_offset_type get_flush_pos() const;

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type min_offset);

  //----

  Status set_commit_pos(slot_offset_type commit_pos);

  slot_offset_type get_commit_pos() const;

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type min_offset);

  //----

  Status close();

 private:
  // See <llfs/file_log_driver/concurrent_shared_state.cpp>
  //
  struct ConcurrentSharedState {
    explicit ConcurrentSharedState(const Location& location_arg, const Config& config_arg) noexcept
        : location{location_arg}
        , config{config_arg}
    {
    }

    // The location of the log files.
    //
    const Location location;

    // The configuration of this driver.
    //
    const Config config;

    // The filenames of all known non-active (sealed) log segments, indexed by slot range.
    //
    batt::Queue<SegmentFile> segments;

    // Log offset pointers.
    //
    batt::Watch<slot_offset_type> trim_pos;
    batt::Watch<slot_offset_type> flush_pos;
    batt::Watch<slot_offset_type> commit_pos;

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

    // Prepare all objects for shutdown.  This function MUST NOT block.
    //
    void halt();
  };

  // See <llfs/file_log_driver/flush_task_main.hpp>
  //
  class FlushTaskMain;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Scans the log directory for segment files, loading them into the ring buffer.
  //
  StatusOr<ActiveFile> recover_segments();

  void start_tasks(ActiveFile&& active_file);

  void trim_task_main();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // The ring buffer state.
  //
  LogStorageDriverContext& context_;

  // Scheduler used to launch the trim and flush tasks.
  //
  batt::TaskScheduler& scheduler_;

  // Thread-safe shared state used to communicate with committers and trimmers.
  //
  ConcurrentSharedState shared_state_;

  // Deletes old segment files when the trim pos is increased.
  //
  Optional<batt::Task> trim_task_;

  // Writes committed data to the active segment file, closing when `this->segment_size_` is
  // reached.
  //
  Optional<batt::Task> flush_task_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class RecoverFileLogDeviceFactory : public LogDeviceFactory
{
 public:
  explicit RecoverFileLogDeviceFactory(const FileLogDriver::Location& location,
                                       batt::TaskScheduler& scheduler) noexcept
      : location_{location}
      , scheduler_{scheduler}

  {
  }

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override
  {
    return FileLogDriver::recover(this->location_, scan_fn, this->scheduler_);
  }

 private:
  FileLogDriver::Location location_;
  batt::TaskScheduler& scheduler_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class InitializeFileLogDeviceFactory : public LogDeviceFactory
{
 public:
  explicit InitializeFileLogDeviceFactory(const FileLogDriver::Location& location,
                                          const FileLogDriver::Config& config,
                                          batt::TaskScheduler& scheduler,
                                          ConfirmThisWillEraseAllMyData confirm) noexcept
      : location_{location}
      , config_{config}
      , scheduler_{scheduler}
      , confirm_{confirm}
  {
  }

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override
  {
    auto device =
        FileLogDriver::initialize(this->location_, this->config_, this->scheduler_, this->confirm_);
    BATT_REQUIRE_OK(device);

    std::unique_ptr<LogDevice::Reader> reader =
        (*device)->new_reader(/*slot_lower_bound=*/None, LogReadMode::kDurable);

    BATT_CHECK_EQ(reader->data().size(), 0u) << "just initialized FileLogDevice; should be empty!";

    StatusOr<slot_offset_type> scan_status = scan_fn(*reader);
    BATT_REQUIRE_OK(scan_status);

    return device;
  }

 private:
  FileLogDriver::Location location_;
  FileLogDriver::Config config_;
  batt::TaskScheduler& scheduler_;
  ConfirmThisWillEraseAllMyData confirm_;
};

}  // namespace llfs

#include <llfs/file_log_driver/active_file.hpp>
#include <llfs/file_log_driver/flush_task_main.hpp>

#endif  // LLFS_FILE_LOG_DRIVER_HPP
