//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/raw_block_device_impl.hpp>
//

#include <llfs/filesystem.hpp>

#include <batteries/async/io_result.hpp>
#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/status.hpp>

#include <utility>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingRawBlockDevice::IoRingRawBlockDevice(IoRing::File&& file) noexcept
    : file_{std::move(file)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingRawBlockDevice::write_some(i64 offset, const ConstBuffer& data) /*override*/
{
  return batt::Task::await<batt::StatusOr<i64>>([&](auto&& handler) {
    this->file_.async_write_some(offset, data, BATT_FORWARD(handler));
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingRawBlockDevice::read_some(i64 offset, const MutableBuffer& buffer) /*override*/
{
  return batt::Task::await<batt::StatusOr<i64>>([&](auto&& handler) {
    this->file_.async_read_some(offset, buffer, BATT_FORWARD(handler));
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingRawBlockDevice::get_size() /*override*/
{
  return sizeof_fd(this->file_.get_fd());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingRawBlockDevice::truncate(i64 new_offset_upper_bound) /*override*/
{
  return truncate_fd(this->file_.get_fd(), BATT_CHECKED_CAST(u64, new_offset_upper_bound));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingRawBlockDevice::truncate_at_least(i64 minimum_size) /*override*/
{
  StatusOr<i64> current_size = this->get_size();
  BATT_REQUIRE_OK(current_size);

  if (*current_size < minimum_size) {
    return this->truncate(minimum_size);
  }

  return batt::OkStatus();
}

}  // namespace llfs
