//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_device_storage.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<DefaultIoRingLogDeviceStorage> DefaultIoRingLogDeviceStorage::make_new(
    MaxQueueDepth entries, int fd)
{
  BATT_ASSIGN_OK_RESULT(IoRing io_ring, IoRing::make_new(entries));
  BATT_ASSIGN_OK_RESULT(IoRing::File file, IoRing::File::open(io_ring, fd));

  return DefaultIoRingLogDeviceStorage{std::move(io_ring), std::move(file)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ DefaultIoRingLogDeviceStorage::DefaultIoRingLogDeviceStorage(
    IoRing&& io_ring, IoRing::File&& file) noexcept
    : io_ring_{std::move(io_ring)}
    , file_{std::move(file)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ DefaultIoRingLogDeviceStorage::EventLoopTask::EventLoopTask(
    DefaultIoRingLogDeviceStorage& storage, std::string_view caller) noexcept
    : storage_{storage}
    , caller_{caller}
    , thread_{[this] {
      LLFS_VLOG(1) << "(" << this->caller_ << ") invoking IoRing::run()";

      Status io_status = this->storage_.io_ring_.run();
      if (!io_status.ok()) {
        LLFS_LOG_WARNING() << "(" << this->caller_ << ") IoRing::run() returned: " << io_status;
      }
      this->done_.set_value(true);

      LLFS_VLOG(1) << "(" << this->caller_ << ") IoRing::run() returned; exiting event loop thread";
    }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DefaultIoRingLogDeviceStorage::EventLoopTask::~EventLoopTask() noexcept
{
  BATT_CHECK(this->join_called_);
  BATT_CHECK(this->done_.get_value());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void DefaultIoRingLogDeviceStorage::EventLoopTask::join()
{
  this->join_called_ = true;
  BATT_CHECK_OK(this->done_.await_equal(true));
  this->thread_.join();
}

}  //namespace llfs
