//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/bloom_filter_page_view.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ PageReader BloomFilterPageView::page_reader()
{
  return [](std::shared_ptr<const PageBuffer> page_buffer)
             -> StatusOr<std::shared_ptr<const PageView>> {
    return {std::make_shared<BloomFilterPageView>(std::move(page_buffer))};
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::Status BloomFilterPageView::register_layout(PageCache& cache)
{
  return cache.register_page_reader(PackedBloomFilterPage::page_layout_id(), __FILE__, __LINE__,
                                    BloomFilterPageView::page_reader());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ BloomFilterPageView::BloomFilterPageView(
    std::shared_ptr<const PageBuffer>&& page_buffer) noexcept
    : PageView{std::move(page_buffer)}
    , packed_{static_cast<const PackedBloomFilterPage*>(this->const_payload().data())}
{
  this->packed_->bloom_filter.check_invariants();
}

}  //namespace llfs
