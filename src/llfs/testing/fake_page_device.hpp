//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_FAKE_PAGE_DEVICE_HPP
#define LLFS_TESTING_FAKE_PAGE_DEVICE_HPP

#include <llfs/memory_page_device.hpp>
#include <llfs/page_device.hpp>

#include <functional>
#include <vector>

namespace llfs {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FakePageDevice : public PageDevice
{
 public:
  static constexpr PageCount kDefaultCapacity{64};
  static constexpr page_device_id_int kDefaultDeviceId = 0x77;
  static constexpr PageSize kDefaultPageSize{4 * kKiB};

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  using PendingOpCompletion = std::function<void(Status)>;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Determines the fake device page count (capacity) and identifier.
  //
  PageIdFactory page_id_factory{kDefaultCapacity, kDefaultDeviceId};

  // The fake device's page size.
  //
  PageSize device_page_size = kDefaultPageSize;

  // The operations currently pending on the fake device.  Tests can choose to invoke these in
  // different orders and with different status codes to simulate arbitrary device behavior.
  //
  std::vector<PendingOpCompletion> pending_ops;

  // When set to a non-Ok status code, `prepare` operations will fail with the given error.
  //
  Status prepare_status;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  PageIdFactory page_ids() override;

  PageSize page_size() override;

  // If `this->prepare_status` is non-ok, returns `this->prepare_status`, else pass the call through
  // to the underlying MemoryPageDevice
  //
  StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) override;

  // Adds a new function to `this->pending_ops`, that when invoked with Status `fake_status`, will
  // invoke `handler` directly if `fake_status.ok() == false`, else invoke `MemoryPageDevice::write`
  // on the underlying implementation with the original args.
  //
  void write(std::shared_ptr<const PageBuffer>&& page_buffer, WriteHandler&& handler) override;

  // Adds a new function to `this->pending_ops`, that when invoked with Status `fake_status`, will
  // invoke `handler` directly if `fake_status.ok() == false`, else invoke `MemoryPageDevice::read`
  // on the underlying implementation with the original args.
  //
  void read(PageId id, ReadHandler&& handler) override;

  // Adds a new function to `this->pending_ops`, that when invoked with Status `fake_status`, will
  // invoke `handler` directly if `fake_status.ok() == false`, else invoke `MemoryPageDevice::drop`
  // on the underlying implementation with the original args.
  //
  void drop(PageId id, WriteHandler&& handler) override;

  // Lazily creates and returns the MemoryPageDevice that serves as the implementation of this
  // device in all non-simulated-error paths.
  //
  MemoryPageDevice& impl();

 private:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // The underlying implementation.
  //
  Optional<MemoryPageDevice> impl_;
};

}  // namespace testing
}  // namespace llfs

#endif  // LLFS_TESTING_FAKE_PAGE_DEVICE_HPP
