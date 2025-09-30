//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_BUFFER_HPP
#define LLFS_PAGE_BUFFER_HPP

#include <llfs/config.hpp>
//
#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_size.hpp>

#include <batteries/static_assert.hpp>

#include <memory>
#include <type_traits>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// An aligned piece of memory used to store a Page.
//
class PageBuffer
{
 public:
  using Block = std::aligned_storage_t<kDirectIOBlockSize, kDirectIOBlockAlign>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Returns the maximum number of bytes available for applications to use within a Page of the
  // given `size`.  This is smaller than the page size because of the standard page header
  // (`PackedPageHeader`) used internally by LLFS.
  //
  static usize max_payload_size(PageSize size);

  // Allocates and returns a new PageBuffer of the specified size with the specified `PageId`.
  //
  // NOTE: `std::shared_ptr` is used here instead of an intrusive ref count (e.g.,
  // batt::SharedPtr/RefCounted) because this class is designed to overlay the page data buffer and
  // nothing else, so that the blocks contained within are properly aligned for direct I/O.
  //
  static std::shared_ptr<PageBuffer> allocate(PageSize size, PageId page_id);

  // Frees a PageBuffer to the pool (or the heap).
  //
  static void deallocate(PageSize page_size, void* ptr);

  // PageBuffer memory is managed internally by LLFS; disable dtor and override the default `delete`
  // operator.
  //
  PageBuffer() = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Returns the size of the page in bytes.  This is the same as the value passed into `allocate`,
  // and includes the size of the `PackedPageHeader` at the front of the memory buffer.
  //
  PageSize size() const;

  // Returns the PageId assigned to this page.
  //
  PageId page_id() const;

  // Sets the PageId of this page.
  //
  void set_page_id(PageId id);

  // Returns a ConstBuffer for the entire page buffer.
  //
  ConstBuffer const_buffer() const;

  // Returns a MutableBuffer for the entire page buffer.
  //
  MutableBuffer mutable_buffer();

  // Returns a ConstBuffer that covers the portion of the page buffer that comes after the
  // `PackedPageHeader`.  This is where application-specific data resides.
  //
  ConstBuffer const_payload() const;

  // Returns a MutableBuffer that covers the portion of the page buffer that comes after the
  // `PackedPageHeader`.  This is the buffer an application would pass to `DataPacker` to format
  // structured data within a page.
  //
  MutableBuffer mutable_payload();

 private:
  Block blocks_[1];
};

BATT_STATIC_ASSERT_EQ(sizeof(PageBuffer), 4096);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageBufferDeleter {
  PageSize page_size;
  PageId page_id;

  void operator()(void* ptr) const
  {
    PageBuffer::deallocate(this->page_size, ptr);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline PageSize get_page_buffer_size(const std::shared_ptr<T>& page_buffer)
{
  const PageBufferDeleter* const deleter = std::get_deleter<PageBufferDeleter>(page_buffer);
  if (deleter) {
    return deleter->page_size;
  }
  return page_buffer->size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PageSize get_page_size(const std::shared_ptr<const PageBuffer>& page_buffer)
{
  return get_page_buffer_size(page_buffer);
}

inline PageSize get_page_size(const std::shared_ptr<PageBuffer>& page_buffer)
{
  return get_page_buffer_size(page_buffer);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline PageId get_page_buffer_page_id(const std::shared_ptr<T>& page_buffer)
{
  const PageBufferDeleter* const deleter = std::get_deleter<PageBufferDeleter>(page_buffer);
  if (deleter) {
    return deleter->page_id;
  }
  return page_buffer->page_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PageId get_page_id(const std::shared_ptr<const PageBuffer>& page_buffer)
{
  return get_page_buffer_page_id(page_buffer);
}

inline PageId get_page_id(const std::shared_ptr<PageBuffer>& page_buffer)
{
  return get_page_buffer_page_id(page_buffer);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline ConstBuffer get_const_buffer(const std::shared_ptr<const PageBuffer>& page_buffer)
{
  return ConstBuffer{page_buffer.get(), get_page_size(page_buffer)};
}

inline ConstBuffer get_const_buffer(const std::shared_ptr<PageBuffer>& page_buffer)
{
  return ConstBuffer{page_buffer.get(), get_page_size(page_buffer)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline MutableBuffer get_mutable_buffer(const std::shared_ptr<PageBuffer>& page_buffer)
{
  return MutableBuffer{page_buffer.get(), get_page_size(page_buffer)};
}

}  // namespace llfs

#endif  // LLFS_PAGE_BUFFER_HPP
