#include <llfs/page_filter.hpp>
//

#include <llfs/pinned_page.hpp>

#include <batteries/case_of.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool NullPageFilter::might_contain_key(const KeyView& /*key*/)
{
  return true;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageBloomFilter::PageBloomFilter(PageId page_id, std::unique_ptr<u64[]> memory) noexcept
    : PageFilter{page_id}
    , memory_{std::move(memory)}
    , filter_{reinterpret_cast<PackedBloomFilter*>(this->memory_.get())}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageBloomFilter::might_contain_key(const KeyView& key)
{
  return filter_->might_contain(key);
}

}  // namespace llfs
