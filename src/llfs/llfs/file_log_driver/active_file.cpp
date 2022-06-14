#include <llfs/file_log_driver.hpp>
//

#include <llfs/filesystem.hpp>

#include <turtle/util/syscall_retry.hpp>

namespace llfs {

StatusOr<FileLogDriver::ActiveFile> FileLogDriver::ActiveFile::open(const Location& location,
                                                                    slot_offset_type base_offset,
                                                                    MutableBuffer buffer) noexcept
{
  fs::path file_path = location.active_segment_file_path();

  if (!fs::exists(file_path)) {
    StatusOr<int> fd = create_active_file(location);
    BATT_REQUIRE_OK(fd);

    return ActiveFile{location, *fd, base_offset, /*size=*/0};
  }

  StatusOr<ConstBuffer> contents = read_file(file_path.string(), buffer);
  BATT_REQUIRE_OK(contents) << batt::LogLevel::kError << "Failed to read contents of " << file_path;

  // Try to open the existing file.
  //
  StatusOr<int> fd = open_file_read_write(file_path.string());
  BATT_REQUIRE_OK(fd) << batt::LogLevel::kError << "Failed to open existing file " << file_path;

  return ActiveFile{location, *fd, base_offset, contents->size()};
}

StatusOr<int> FileLogDriver::ActiveFile::create_active_file(const Location& location)
{
  return create_file_read_write(location.active_segment_file_path().string());
}

FileLogDriver::ActiveFile::ActiveFile(const Location& location, int fd,
                                      slot_offset_type base_offset, std::size_t size) noexcept
    : location_{location}
    , fd_{fd}
    , slot_range_{
          .lower_bound = base_offset,
          .upper_bound = base_offset + size,
      }
{
}

FileLogDriver::ActiveFile::ActiveFile(ActiveFile&& that) noexcept
    : location_{that.location_}
    , fd_{that.fd_}
    , slot_range_{that.slot_range_}
{
  that.release();
}

FileLogDriver::ActiveFile& FileLogDriver::ActiveFile::operator=(ActiveFile&& that) noexcept
{
  if (this != &that) {
    this->close();
    this->location_ = std::move(that.location_);
    this->fd_ = that.fd_;
    this->slot_range_ = that.slot_range_;
    that.release();
  }
  return *this;
}

FileLogDriver::ActiveFile::~ActiveFile() noexcept
{
  this->close();
}

// Release ownership of the underlying file descriptor.
//
void FileLogDriver::ActiveFile::release()
{
  this->fd_ = -1;
  this->slot_range_ = SlotRange{0, 0};
}

void FileLogDriver::ActiveFile::close() noexcept
{
  if (this->fd_ != -1) {
    turtle_db::syscall_retry([&] {
      return ::close(this->fd_);
    });
    this->fd_ = -1;
  }
}

Status FileLogDriver::ActiveFile::truncate(u64 bytes_to_drop_from_end)
{
  BATT_CHECK_NE(this->fd_, -1);

  const off_t current_size = turtle_db::syscall_retry([&] {
    return ::lseek(this->fd_, /*offset=*/0, /*whence=*/SEEK_END);
  });
  BATT_CHECK_LE(bytes_to_drop_from_end, static_cast<u64>(current_size));

  const off_t target_size = current_size - bytes_to_drop_from_end;

  const int retval = turtle_db::syscall_retry([&] {
    return ::ftruncate(this->fd_, /*length=*/target_size);
  });
  BATT_REQUIRE_OK(status_from_retval(retval));

  this->slot_range_.upper_bound = this->slot_range_.lower_bound + target_size;

  return OkStatus();
}

Status FileLogDriver::ActiveFile::append(ConstBuffer buffer)
{
  while (buffer.size() > 0) {
    const auto bytes_written = turtle_db::syscall_retry([&] {
      return ::pwrite(this->fd_, buffer.data(), buffer.size(), /*offset=*/this->slot_range_.size());
    });
    BATT_REQUIRE_OK(status_from_retval(bytes_written));

    buffer += bytes_written;
    this->slot_range_.upper_bound += bytes_written;
  }

  return status_from_retval(turtle_db::syscall_retry([&] {
    return fsync(this->fd_);
  }));
}

StatusOr<FileLogDriver::SegmentFile> FileLogDriver::ActiveFile::split()
{
  BATT_CHECK_NE(this->fd_, -1) << "split may only be called on an open ActiveFile!";

  this->close();

  // Create a SegmentFile object to return.
  //
  SegmentFile finished_segment{
      .slot_range = this->slot_range_,
      .file_name = this->location_.segment_file_name_from_slot_range(this->slot_range_),
  };

  // Set the name of the now finalized active segment file to reflect its slot range.
  //
  int retval = turtle_db::syscall_retry([&] {
    return ::rename(/*from=*/this->location_.active_segment_file_path().c_str(),
                    /*to=*/finished_segment.file_name.c_str());
  });
  BATT_REQUIRE_OK(status_from_retval(retval));

  // Advance the active slot range.
  //
  this->slot_range_.lower_bound = this->slot_range_.upper_bound;

  // Create a new active file for writing.
  //
  StatusOr<int> new_fd = create_active_file(this->location_);
  BATT_REQUIRE_OK(new_fd);

  this->fd_ = *new_fd;

  // Success!  Return the last segment file.
  //
  return finished_segment;
}

}  // namespace llfs
