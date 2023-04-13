//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_page_device.hpp>
//

#include <llfs/simulated_page_device_impl.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedPageDevice::SimulatedPageDevice(std::shared_ptr<Impl>&& impl) noexcept
    : impl_{std::move(impl)}
    , create_step_{this->impl_->simulation().current_step()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageIdFactory SimulatedPageDevice::page_ids() /*override*/
{
  return this->impl_->page_ids();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageSize SimulatedPageDevice::page_size() /*override*/
{
  return this->impl_->page_size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> SimulatedPageDevice::prepare(PageId page_id) /*override*/
{
  return this->impl_->prepare(this->create_step_, page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::write(std::shared_ptr<const PageBuffer>&& page_buffer,
                                WriteHandler&& handler) /*override*/
{
  return this->impl_->write(this->create_step_, std::move(page_buffer), std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::read(PageId id, ReadHandler&& handler) /*override*/
{
  return this->impl_->read(this->create_step_, id, std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::drop(PageId id, WriteHandler&& handler) /*override*/
{
  return this->impl_->drop(this->create_step_, id, std::move(handler));
}

}  //namespace llfs
