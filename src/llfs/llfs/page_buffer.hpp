#pragma once
#ifndef LLFS_PAGE_BUFFER_HPP
#define LLFS_PAGE_BUFFER_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_size.hpp>

#include <memory>
#include <type_traits>

namespace llfs {

class PageBuffer
{
 public:
  using Block = std::aligned_storage_t<4096, 512>;

  static usize max_payload_size(PageSize size);

  static std::shared_ptr<PageBuffer> allocate(usize size, PageId page_id = PageId{kInvalidPageId});

  PageBuffer() = delete;

  usize size() const;

  PageId page_id() const;

  void set_page_id(PageId id);

  ConstBuffer const_buffer() const;

  MutableBuffer mutable_buffer();

  ConstBuffer const_payload() const;

  MutableBuffer mutable_payload();

  void operator delete(void* ptr);

 private:
  Block blocks_[1];
};

}  // namespace llfs

#endif  // LLFS_PAGE_BUFFER_HPP
