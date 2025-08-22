//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_buffer.hpp>
//

#include <llfs/config.hpp>
#include <llfs/page_layout.hpp>

#include <batteries/env.hpp>
#include <batteries/math.hpp>
#include <batteries/suppress.hpp>

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// PageBuffer object memory cache.
//
namespace {

using Pool = boost::lockfree::queue<PageBuffer*, boost::lockfree::capacity<kPageBufferPoolSize>,
                                    boost::lockfree::fixed_sized<true>>;

struct PoolContext {
  Pool arena;
  std::atomic<usize> size{0};
};

bool enable_page_buffer_pool()
{
  static const bool b_ = [] {
    const char* const varname = "LLFS_ENABLE_PAGE_BUFFER_POOL";
    const bool b = batt::getenv_as<bool>(varname).value_or(kEnablePageBufferPoolByDefault);
    LOG(INFO) << varname << "=" << b;
    return b;
  }();

  return b_;
}

static PoolContext& pool_for_size(usize size)
{
  static std::array<PoolContext*, kPageBufferPoolLevels>& context_ = []() -> decltype(auto) {
    BATT_CHECK(enable_page_buffer_pool());

    static std::array<PoolContext*, kPageBufferPoolLevels> context_;
    context_.fill(nullptr);
    for (auto& c : context_) {
      c = new PoolContext;
    }
    return (context_);
  }();

  PoolContext* p = context_[batt::log2_ceil(size)];
  BATT_CHECK_NOT_NULLPTR(p);
  return *p;
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageSize PageBuffer::size() const
{
  return PageSize{get_page_header(*this).size};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageId PageBuffer::page_id() const
{
  return PageId{get_page_header(*this).page_id.id_val};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageBuffer::set_page_id(PageId id)
{
  mutable_page_header(this)->page_id.id_val = id.int_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ usize PageBuffer::max_payload_size(PageSize size)
{
  return size - sizeof(PackedPageHeader);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ std::shared_ptr<PageBuffer> PageBuffer::allocate(PageSize page_size, PageId page_id)
{
  PageBuffer* obj = nullptr;
  [&obj, page_size] {
    if (enable_page_buffer_pool()) {
      PoolContext& pool = pool_for_size(page_size);
      if (pool.arena.pop(obj)) {
        pool.size.fetch_sub(1);
        BATT_CHECK_NOT_NULLPTR(obj);
        BATT_CHECK_EQ(page_size, get_page_header(*obj).size);
        return;
      }
    }

    const usize n_blocks = (page_size + sizeof(Block) - 1) / sizeof(Block);
    Block* blocks = new Block[n_blocks];
    obj = reinterpret_cast<PageBuffer*>(blocks);
  }();
  {
    PackedPageHeader* header = mutable_page_header(obj);
    header->size = page_size;
    header->magic = PackedPageHeader::kMagic;
    header->crc32 = PackedPageHeader::kCrc32NotSet;
    header->unused_begin = page_size;
    header->unused_end = page_size;
  }

  obj->set_page_id(page_id);

  return std::shared_ptr<PageBuffer>{obj, PageBufferDeleter{page_size, page_id}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ void PageBuffer::deallocate(PageSize page_size, void* ptr)
{
  if (enable_page_buffer_pool()) {
    PageBuffer* obj = reinterpret_cast<PageBuffer*>(ptr);
    if (obj) {
      auto& pool = pool_for_size(page_size);
      mutable_page_header(obj)->size = page_size;
      if (pool.arena.push(obj)) {
        pool.size.fetch_add(1);
        return;
      }
    }
  }
  BATT_SUPPRESS_IF_CLANG("-Wunevaluated-expression")
  delete[] reinterpret_cast<decltype(new Block[1])>(ptr);
  BATT_UNSUPPRESS_IF_CLANG()
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer PageBuffer::const_buffer() const
{
  return ConstBuffer{this, this->size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer PageBuffer::mutable_buffer()
{
  return MutableBuffer{this, this->size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer PageBuffer::const_payload() const
{
  return this->const_buffer() + sizeof(PackedPageHeader);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer PageBuffer::mutable_payload()
{
  return this->mutable_buffer() + sizeof(PackedPageHeader);
}

}  // namespace llfs
