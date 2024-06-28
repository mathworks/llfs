//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/filesystem.hpp>
//

#include <llfs/track_fds.hpp>

#include <batteries/finally.hpp>
#include <batteries/syscall_retry.hpp>

#include <sys/types.h>
#include <unistd.h>

namespace llfs {

using ::batt::syscall_retry;

namespace {

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int system_open2(const char* path, int flags)
{
  return maybe_track_fd(::open(path, flags));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int system_open3(const char* path, int flags, int mode)
{
  return maybe_track_fd(::open(path, flags, mode));
}

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int build_open_flags(OpenForRead open_for_read, OpenForWrite open_for_write,
                     OpenForAppend open_for_append, OpenRawIO open_raw_io)
{
  int flags = 0;

  if (open_for_read && open_for_write) {
    flags |= O_RDWR;
  } else if (open_for_read) {
    flags |= O_RDONLY;
  } else if (open_for_write) {
    flags |= O_WRONLY;
  }

  if (open_for_write && open_for_append) {
    flags |= O_APPEND;
  }

  if (open_raw_io) {
#ifdef LLFS_PLATFORM_IS_LINUX
    flags |= O_DIRECT | O_SYNC;
#else
    LLFS_LOG_WARNING() << "open_raw_io only supported on Linux!";
#endif
  }

  return flags;
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<int> open_file_read_only(std::string_view file_name, OpenRawIO open_raw_io)
{
  int flags = build_open_flags(OpenForRead{true},     //
                               OpenForWrite{false},   //
                               OpenForAppend{false},  //
                               open_raw_io);

  const int fd = syscall_retry([&] {
    return system_open2(std::string(file_name).c_str(), flags);
  });

  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<int> open_file_read_write(std::string_view file_name, OpenForAppend open_for_append,
                                   OpenRawIO open_raw_io)
{
  int flags = build_open_flags(OpenForRead{true},   //
                               OpenForWrite{true},  //
                               open_for_append,     //
                               open_raw_io);

  const int fd = syscall_retry([&] {
    return system_open2(std::string(file_name).c_str(), flags);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<int> create_file_read_write(std::string_view file_name, OpenForAppend open_for_append)
{
  int flags = build_open_flags(OpenForRead{true},   //
                               OpenForWrite{true},  //
                               open_for_append,     //
                               OpenRawIO{false})    //
              | O_CREAT | O_EXCL | O_TRUNC;

  const int fd = syscall_retry([&] {
    return system_open3(std::string(file_name).c_str(), flags, /*mode=*/0644);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status truncate_file(std::string_view file_name, u64 size)
{
  const int fd = syscall_retry([&] {
    return system_open3(std::string(file_name).c_str(), O_RDWR | O_CREAT, 0644);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  auto close_fd = batt::finally([&] {
    ::close(fd);
  });

  BATT_REQUIRE_OK(truncate_fd(fd, size));

  const i64 new_size = syscall_retry([&] {
    return ::lseek(fd, 0, SEEK_END);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(new_size));

  BATT_CHECK_EQ(new_size, (i64)size);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status truncate_fd(int fd, u64 size)
{
  const int retval = syscall_retry([&] {
    return ::ftruncate(fd, size);
  });
  return batt::status_from_retval(retval);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ConstBuffer> read_file(std::string_view file_name, MutableBuffer buffer, u64 offset)
{
  StatusOr<int> fd = open_file_read_only(file_name);
  BATT_REQUIRE_OK(fd);

  // Don't leak the file descriptor!
  //
  auto closer = batt::finally([fd] {
    (void)syscall_retry([&] {
      return ::close(*fd);
    });
  });

  return read_fd(*fd, buffer, offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ConstBuffer> read_fd(int fd, MutableBuffer buffer, u64 offset)
{
  ConstBuffer contents{buffer.data(), /*size=*/0};
  while (buffer.size() > 0) {
    const auto bytes_read = syscall_retry([&] {
      return ::pread(fd, buffer.data(), buffer.size(), offset + contents.size());
    });
    BATT_REQUIRE_OK(batt::status_from_retval(bytes_read));

    // Detect end-of-file.
    //
    if (bytes_read == 0) {
      break;
    }

    contents = ConstBuffer{contents.data(), contents.size() + bytes_read};
    buffer += bytes_read;
  }

  return contents;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status write_fd(int fd, ConstBuffer buffer, u64 offset)
{
  while (buffer.size() > 0) {
    const auto bytes_written = syscall_retry([&] {
      return ::pwrite(fd, buffer.data(), buffer.size(), offset);
    });
    BATT_REQUIRE_OK(batt::status_from_retval(bytes_written));

    // Something has gone wrong...
    //
    if (bytes_written == 0) {
      break;
    }

    buffer += bytes_written;
    offset += bytes_written;
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status close_fd(int fd)
{
  const int retval = syscall_retry([&] {
    return ::close(fd);
  });
  return batt::status_from_retval(retval);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status delete_file(std::string_view file_name)
{
  return batt::status_from_retval(syscall_retry([&] {
    return ::unlink(std::string(file_name).c_str());
  }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> sizeof_file(std::string_view file_name)
{
  StatusOr<int> fd = open_file_read_only(file_name);
  BATT_REQUIRE_OK(fd);

  const auto close_fd = batt::finally([fd] {
    ::close(*fd);
  });

  return sizeof_fd(*fd);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> sizeof_fd(int fd)
{
  const auto original = syscall_retry([&] {
    return ::lseek(fd, 0, SEEK_CUR);
  });

  BATT_REQUIRE_OK(batt::status_from_retval(original));

  const auto retval = syscall_retry([&] {
    return ::lseek(fd, 0, SEEK_END);
  });

  BATT_REQUIRE_OK(batt::status_from_retval(retval));

  const auto restore = syscall_retry([&] {
    return ::lseek(fd, original, SEEK_SET);
  });

  BATT_REQUIRE_OK(batt::status_from_retval(restore));

  return retval;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> get_fd_flags(int fd)
{
  const int retval = batt::syscall_retry([&] {
    return ::fcntl(fd, F_GETFD);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(retval));

  return retval;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status set_fd_flags(int fd, i32 flags)
{
  const int retval = batt::syscall_retry([&] {
    return ::fcntl(fd, F_SETFD, flags);
  });
  return batt::status_from_retval(retval);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status update_fd_flags(int fd, EnableFileFlags enable_flags, DisableFileFlags disable_flags)
{
  StatusOr<i32> current_flags = get_fd_flags(fd);
  BATT_REQUIRE_OK(current_flags);

  const i32 desired_flags = (*current_flags | i32{enable_flags}) & ~i32{disable_flags};
  return set_fd_flags(fd, desired_flags);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> get_file_status_flags(int fd)
{
  const int retval = batt::syscall_retry([&] {
    return ::fcntl(fd, F_GETFL);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(retval));

  return retval;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status set_file_status_flags(int fd, i32 flags)
{
  const int retval = batt::syscall_retry([&] {
    return ::fcntl(fd, F_SETFL, flags);
  });
  return batt::status_from_retval(retval);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status update_file_status_flags(int fd, EnableFileFlags enable_flags,
                                DisableFileFlags disable_flags)
{
  StatusOr<i32> current_flags = get_file_status_flags(fd);
  BATT_REQUIRE_OK(current_flags);

  const i32 desired_flags = (*current_flags | i32{enable_flags}) & ~i32{disable_flags};
  return set_file_status_flags(fd, desired_flags);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status enable_raw_io_fd(int fd, bool enabled)
{
#ifdef LLFS_PLATFORM_IS_LINUX  //----- --- -- -  -  -   -

  // TODO [tastolfi 2022-06-21] Add O_SYNC/O_DSYNC to the flags masks below once Linux supports
  // this (https://man7.org/linux/man-pages/man2/fcntl.2.html#BUGS)
  //
  if (enabled) {
    return update_file_status_flags(fd, EnableFileFlags{O_DIRECT}, DisableFileFlags{0});
  } else {
    return update_file_status_flags(fd, EnableFileFlags{0}, DisableFileFlags{O_DIRECT});
  }

#else  //----- --- -- -  -  -   -

  (void)fd;
  (void)enabled;

  LLFS_LOG_WARNING() << "enable_raw_io_fd only supported on Linux!";

  return OkStatus();

#endif  //----- --- -- -  -  -   -
}

}  // namespace llfs
