//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/testing/fake_page_device.hpp>
//

namespace llfs {
namespace testing {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageIdFactory FakePageDevice::page_ids() /*override*/
{
  return this->page_id_factory;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u32 FakePageDevice::page_size() /*override*/
{
  return this->device_page_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> FakePageDevice::prepare(PageId page_id) /*override*/
{
  BATT_REQUIRE_OK(prepare_status);

  return this->impl().prepare(page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FakePageDevice::write(std::shared_ptr<const PageBuffer>&& page_buffer,
                           WriteHandler&& handler) /*override*/
{
  this->pending_ops.emplace_back([this, page_buffer = std::move(page_buffer),
                                  handler = std::move(handler)](Status status) mutable {
    if (!status.ok()) {
      handler(status);
    } else {
      this->write(std::move(page_buffer), std::move(handler));
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FakePageDevice::read(PageId id, ReadHandler&& handler) /*override*/
{
  this->pending_ops.emplace_back([this, id, handler = std::move(handler)](Status status) mutable {
    if (!status.ok()) {
      handler(status);
    } else {
      this->read(id, std::move(handler));
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FakePageDevice::drop(PageId id, WriteHandler&& handler) /*override*/
{
  this->pending_ops.emplace_back([this, id, handler = std::move(handler)](Status status) mutable {
    if (!status.ok()) {
      handler(status);
    } else {
      this->drop(id, std::move(handler));
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemoryPageDevice& FakePageDevice::impl()
{
  if (!this->impl_) {
    this->impl_.emplace(this->page_ids().get_device_id(),
                        this->page_ids().get_physical_page_count(), this->page_size());
  }

  // Once `impl_` has been created, the page size, page count, and device id can't be changed!
  //
  BATT_CHECK_EQ(this->impl_->page_size(), this->page_size());
  BATT_CHECK_EQ(this->impl_->page_ids(), this->page_ids());

  return *this->impl_;
}

}  // namespace testing
}  // namespace llfs
