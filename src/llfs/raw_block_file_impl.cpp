//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/raw_block_file_impl.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/filesystem.hpp>

#include <batteries/async/io_result.hpp>
#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/status.hpp>
#include <batteries/stream_util.hpp>
#include <batteries/syscall_retry.hpp>

#include <boost/io/ios_state.hpp>

#include <utility>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<IoRingRawBlockFile>> IoRingRawBlockFile::open(
    const IoRing& io_ring, const char* file_name, int flags, Optional<mode_t> mode)
{
  const int fd = batt::syscall_retry([&] {
    return system_open3(file_name, flags, mode.value_or(0644));
  });
  BATT_REQUIRE_OK(batt::status_from_retval(fd));

  return std::make_unique<IoRingRawBlockFile>(IoRing::File{io_ring, fd});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingRawBlockFile::IoRingRawBlockFile(IoRing::File&& file) noexcept
    : file_{std::move(file)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingRawBlockFile::write_some(i64 offset, const ConstBuffer& data) /*override*/
{
  LLFS_VLOG(1) << "IoRingRawBlockFile::write_some(" << BATT_INSPECT(offset) << ", "
               << BATT_INSPECT(data.size()) << ")";
  LLFS_VLOG(2) << "  data=" << batt::dump_hex(data.data(), data.size());

  StatusOr<i64> result = batt::Task::await<batt::StatusOr<i64>>([&](auto&& handler) {
    this->file_.async_write_some(offset, data, BATT_FORWARD(handler));
  });

  BATT_REQUIRE_OK(result) << BATT_INSPECT(offset) << BATT_INSPECT(data.data())
                          << BATT_INSPECT(data.size()) << boost::stacktrace::stacktrace{};

  /*
  const int retval = batt::syscall_retry([&] {
    return ::fdatasync(this->file_.get_fd());
  });
  BATT_REQUIRE_OK(batt::status_from_retval(retval));
  */
  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingRawBlockFile::read_some(i64 offset, const MutableBuffer& buffer) /*override*/
{
  LLFS_VLOG(1) << "IoRingRawBlockFile::read_some(" << BATT_INSPECT(offset) << ", "
               << BATT_INSPECT(buffer.size()) << ")";

  return batt::Task::await<batt::StatusOr<i64>>([&](auto&& handler) {
    this->file_.async_read_some(offset, buffer, BATT_FORWARD(handler));
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingRawBlockFile::get_size() /*override*/
{
  return sizeof_fd(this->file_.get_fd());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingRawBlockFile::truncate(i64 new_offset_upper_bound) /*override*/
{
  return truncate_fd(this->file_.get_fd(), BATT_CHECKED_CAST(u64, new_offset_upper_bound));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingRawBlockFile::truncate_at_least(i64 minimum_size) /*override*/
{
  StatusOr<i64> current_size = this->get_size();
  BATT_REQUIRE_OK(current_size);

  if (*current_size < minimum_size) {
    return this->truncate(minimum_size);
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingRawBlockFile::close()
{
  return close_fd(this->file_.release());
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
