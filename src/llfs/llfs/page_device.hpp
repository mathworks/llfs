#pragma once
#ifndef LLFS_PAGE_DEVICE_HPP
#define LLFS_PAGE_DEVICE_HPP

#include <llfs/page_buffer.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/status.hpp>

#include <glog/logging.h>

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

  using WriteHandler = std::function<void(WriteResult)>;
  using ReadHandler = std::function<void(ReadResult)>;

  PageDevice(const PageDevice&) = delete;
  PageDevice& operator=(const PageDevice&) = delete;

  virtual ~PageDevice() = default;

  virtual PageIdFactory page_ids() = 0;

  // TODO [tastolfi 2021-09-07] should this be 64 bit?  store as log2?
  //
  virtual u32 page_size() = 0;

  // For convenience...
  //
  u64 capacity()
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

  virtual void write(std::shared_ptr<const PageBuffer>&& page_buffer, WriteHandler&& handler) = 0;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Read phase
  //
  virtual void read(PageId id, ReadHandler&& handler) = 0;

  // Convenience; shortcut for Task::await(...)
  //
  ReadResult await_read(PageId id);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Delete phase
  //
  virtual void drop(PageId id, WriteHandler&& handler) = 0;

 protected:
  PageDevice() = default;
};

}  // namespace llfs

#endif  // LLFS_PAGE_DEVICE_HPP
