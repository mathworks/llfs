#pragma once
#ifndef LLFS_FILE_LOG_DRIVER_ACTIVE_FILE_HPP
#define LLFS_FILE_LOG_DRIVER_ACTIVE_FILE_HPP

namespace llfs {

class FileLogDriver::ActiveFile
{
 public:
  // Open an existing active segment file or create a new one.  Any existing data will be read into
  // `buffer` if an active segment file is found.
  //
  static StatusOr<ActiveFile> open(const Location& location, slot_offset_type base_offset,
                                   MutableBuffer buffer) noexcept;

  ActiveFile() = default;

  ActiveFile(const ActiveFile&) = delete;
  ActiveFile& operator=(const ActiveFile&) = delete;

  ActiveFile(ActiveFile&&) noexcept;
  ActiveFile& operator=(ActiveFile&&) noexcept;

  // Closes any open file descriptors owned by this object.
  //
  ~ActiveFile() noexcept;

  // True iff this refers to a valid, open segment file.
  //
  explicit operator bool() const noexcept
  {
    return this->fd_ != -1;
  }

  // This is done on recovery, if we detect partially written data; drop the specified number of
  // bytes from the active segment file.
  //
  Status truncate(u64 bytes_to_drop_from_end);

  // Write and flush the given bytes to the active segment file.
  //
  Status append(ConstBuffer buffer);

  // Close the file; invalidates this object.
  //
  void close() noexcept;

  // The current size (flushed, on disk) of the file.
  //
  u64 size() const
  {
    return this->slot_range_.size();
  }

  // The log slot offset range currently covered by this file.
  //
  const SlotRange& slot_range() const
  {
    return this->slot_range_;
  }

  // Finalize the contents of this file and move on to the next one.
  //
  StatusOr<SegmentFile> split();

 private:
  // Create a new active file and return its file descriptor.
  //
  static StatusOr<int> create_active_file(const Location& location);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Create a new ActiveFile object.
  //
  explicit ActiveFile(const Location& location, int fd, slot_offset_type base_offset,
                      std::size_t size) noexcept;

  // Release ownership of the underlying file descriptor.
  //
  void release();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // The location of the log files.
  //
  Location location_;

  // The file descriptor for the active file.
  //
  int fd_ = -1;

  // The current SlotRange interval for the active file.  The upper_bound will advance as more data
  // is flushed.
  //
  SlotRange slot_range_{0, 0};
};

}  // namespace llfs

#endif  // LLFS_FILE_LOG_DRIVER_ACTIVE_FILE_HPP
