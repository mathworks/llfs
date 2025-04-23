//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_DEVICE_HPP
#define LLFS_PAGE_DEVICE_HPP

#include <llfs/page_buffer.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/status.hpp>

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>
#include <batteries/stream_util.hpp>

#include <functional>
#include <memory>
#include <tuple>

namespace llfs {

class PageDevice
{
 public:
  using WriteResult = Status;
  using ReadResult = StatusOr<std::shared_ptr<const PageBuffer>>;

  using WriteHandler = std::function<void(PageDevice::WriteResult)>;
  using ReadHandler = std::function<void(PageDevice::ReadResult)>;

  PageDevice(const PageDevice&) = delete;
  PageDevice& operator=(const PageDevice&) = delete;

  virtual ~PageDevice() = default;

  virtual PageIdFactory page_ids() = 0;

  virtual PageSize page_size() = 0;

  virtual bool get_last_in_file() const { return false; }

  // For convenience...
  //
  PageCount capacity()
  {
    return this->page_ids().get_physical_page_count();
  }

  page_device_id_int get_id()
  {
    return this->page_ids().get_device_id();
  }

  auto summary()
  {
    return [this](std::ostream& out) {
      out << "PageDevice{.id=" << this->get_id() << ", .page_size=" << this->page_size()
          << ", .capacity=" << this->capacity() << ",}";
    };
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Write phase
  //
  virtual StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) = 0;

  virtual void write(std::shared_ptr<const PageBuffer>&& page_buffer,
                     PageDevice::WriteHandler&& handler) = 0;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Read phase
  //
  virtual void read(PageId id, PageDevice::ReadHandler&& handler) = 0;

  // Convenience; shortcut for Task::await(...)
  //
  PageDevice::ReadResult await_read(PageId id);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Delete phase
  //
  virtual void drop(PageId id, PageDevice::WriteHandler&& handler) = 0;

 protected:
  PageDevice() = default;
};

}  // namespace llfs

#endif  // LLFS_PAGE_DEVICE_HPP
