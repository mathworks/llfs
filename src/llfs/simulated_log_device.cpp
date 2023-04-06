//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_log_device.hpp>
//

#include <llfs/simulated_log_device_impl.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDevice::SimulatedLogDevice(std::shared_ptr<Impl>&& impl) noexcept
    : impl_{std::move(impl)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 SimulatedLogDevice::capacity() const /*override*/
{
  return this->impl_->capacity();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 SimulatedLogDevice::size() const /*override*/
{
  return this->impl_->size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::trim(slot_offset_type slot_lower_bound) /*override*/
{
  return this->impl_->trim(slot_lower_bound);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<LogDevice::Reader> SimulatedLogDevice::new_reader(
    Optional<slot_offset_type> slot_lower_bound, LogReadMode mode) /*override*/
{
  return this->impl_->new_reader(slot_lower_bound, mode);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotRange SimulatedLogDevice::slot_range(LogReadMode mode) /*override*/
{
  return this->impl_->slot_range(mode);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LogDevice::Writer& SimulatedLogDevice::writer() /*override*/
{
  return this->impl_->writer();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::close() /*override*/
{
  return this->impl_->close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::sync(LogReadMode mode, SlotUpperBoundAt event) /*override*/
{
  return this->impl_->sync(mode, event);
}

}  //namespace llfs
