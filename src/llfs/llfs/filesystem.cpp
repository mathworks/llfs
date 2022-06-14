#include <llfs/filesystem.hpp>
//

#include <turtle/util/syscall_retry.hpp>

#include <batteries/finally.hpp>

#include <sys/types.h>
#include <unistd.h>

namespace llfs {

// TODO [tastolfi 2022-01-03] move `syscall_retry` into a more core namespace/library.
//
using ::turtle_db::syscall_retry;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<int> open_file_read_only(std::string_view file_name)
{
  const int fd = syscall_retry([&] {
    return ::open(std::string(file_name).c_str(), O_RDONLY);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<int> open_file_read_write(std::string_view file_name, OpenForAppend open_for_append)
{
  int flags = O_RDWR;
  if (open_for_append) {
    flags |= O_APPEND;
  }
  const int fd = syscall_retry([&] {
    return ::open(std::string(file_name).c_str(), flags);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<int> create_file_read_write(std::string_view file_name, OpenForAppend open_for_append)
{
  int flags = O_RDWR | O_CREAT | O_EXCL | O_TRUNC;
  if (open_for_append) {
    flags |= O_APPEND;
  }
  const int fd = syscall_retry([&] {
    return ::open(std::string(file_name).c_str(), flags, /*mode=*/0644);
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status truncate_file(std::string_view file_name, u64 size)
{
  const int fd = syscall_retry([&] {
    return ::open(std::string(file_name).c_str(), O_RDWR | O_CREAT, 0644);
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

}  // namespace llfs
