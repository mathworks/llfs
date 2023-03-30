//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <batteries/suppress.hpp>
BATT_SUPPRESS_IF_GCC("-Wmaybe-uninitialized");

#include <llfs/page_recycler_recovery_visitor.hpp>
//

#include <llfs/status_code.hpp>

#include <boost/uuid/uuid_generators.hpp>

#include <algorithm>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageRecyclerRecoveryVisitor::PageRecyclerRecoveryVisitor(
    const PageRecyclerOptions& default_options) noexcept
    : options_{default_options}
    , recovered_pages_{}
    , latest_batch_slot_{}
    , latest_batch_pages_{}
    , recycler_uuid_{boost::uuids::random_generator{}()}
    , latest_info_refresh_slot_{}
{
  initialize_status_codes();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageRecyclerOptions& PageRecyclerRecoveryVisitor::options() const
{
  return this->options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<PageToRecycle> PageRecyclerRecoveryVisitor::recovered_pages() const
{
  std::vector<PageToRecycle> to_recycle;

  for (const auto& [page_id, page_to_recycle] : this->recovered_pages_) {
    to_recycle.emplace_back(page_to_recycle);
  }
  std::sort(to_recycle.begin(), to_recycle.end(), SlotOffsetOrder{});

  return to_recycle;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Optional<PageRecycler::Batch>> PageRecyclerRecoveryVisitor::consume_latest_batch()
{
  Optional<i32> depth = None;

  if (!this->latest_batch_slot_) {
    return {None};
  }

  PageRecycler::Batch batch;
  batch.slot_offset = *this->latest_batch_slot_;
  for (PageId page_id : this->latest_batch_pages_) {
    // Find the page in `recovered_pages_` to retrieve its discovery depth.
    //
    auto iter = this->recovered_pages_.find(page_id);
    if (iter == this->recovered_pages_.end()) {
      LLFS_LOG_WARNING() << "kBatchContainsUnknownPageDuringRecovery: " << page_id;
      return ::llfs::make_status(StatusCode::kBatchContainsUnknownPageDuringRecovery);
    }

    if (!depth) {
      depth.emplace(iter->second.depth);
    } else {
      const i32 actual_depth = depth.value_or(-1);
      const i32 page_depth = iter->second.depth;
      BATT_CHECK_EQ(actual_depth, page_depth);
    }

    // Transfer the page from the recovered pages map to the batch.
    //
    batch.to_recycle.emplace_back(iter->second);
    this->recovered_pages_.erase(iter);
  }
  batch.depth = depth.value_or(0);
  this->latest_batch_slot_ = None;
  this->latest_batch_pages_.clear();

  return batch;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const boost::uuids::uuid& PageRecyclerRecoveryVisitor::recycler_uuid() const
{
  return this->recycler_uuid_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<SlotRange> PageRecyclerRecoveryVisitor::latest_info_refresh_slot() const
{
  return this->latest_info_refresh_slot_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageRecyclerRecoveryVisitor::operator()(const SlotParse& slot,
                                               const PageToRecycle& to_recycle)
{
  LLFS_VLOG(1) << "Recovered slot: |" << slot.offset << "| " << to_recycle;

  this->recovered_pages_[to_recycle.page_id] = to_recycle;
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageRecyclerRecoveryVisitor::operator()(const SlotParse& slot,
                                               const PackedRecyclePagePrepare& prepare)
{
  LLFS_VLOG(1) << "Recovered slot: |" << slot.offset << "| " << prepare;

  // Validate the new prepared page against the current batch (or start one).
  //
  if (this->latest_batch_slot_) {
    if (prepare.batch_slot != *this->latest_batch_slot_) {
      LLFS_LOG_WARNING() << "kTooManyPendingBatchesDuringRecovery"
                         << BATT_INSPECT(prepare.batch_slot)
                         << BATT_INSPECT(*this->latest_batch_slot_);
      return ::llfs::make_status(StatusCode::kTooManyPendingBatchesDuringRecovery);
    }
  } else {
    BATT_CHECK(this->latest_batch_pages_.empty());
    this->latest_batch_slot_ = prepare.batch_slot;
  }

  this->latest_batch_pages_.emplace_back(PageId{prepare.page_id.value()});

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageRecyclerRecoveryVisitor::operator()(const SlotParse& slot,
                                               const PackedRecycleBatchCommit& commit)
{
  LLFS_VLOG(1) << "Recovered slot: |" << slot.offset << "| " << commit;

  // It's OK if the latest prepared batch has been trimmed off the log; but if we *can* see it, then
  // validate that the batch slots match here.
  //
  if (this->latest_batch_slot_ && *this->latest_batch_slot_ != commit.batch_slot) {
    LLFS_LOG_WARNING() << "kInvalidBatchCommitDuringRecovery: "
                       << BATT_INSPECT(this->latest_batch_slot_) << BATT_INSPECT(commit.batch_slot);
    return llfs::make_status(StatusCode::kInvalidBatchCommitDuringRecovery);
  }
  this->latest_batch_slot_ = None;
  this->latest_batch_pages_.clear();
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageRecyclerRecoveryVisitor::operator()(const SlotParse& slot,
                                               const PackedPageRecyclerInfo& info)
{
  LLFS_VLOG(1) << "Recovered slot: |" << slot.offset << "| " << info;

  this->latest_info_refresh_slot_ = slot.offset;

  this->recycler_uuid_ = info.uuid;

  this->options_.set_info_refresh_rate(info.info_refresh_rate);
  this->options_.set_max_refs_per_page(info.max_refs_per_page);
  this->options_.set_batch_size(info.batch_size);
  this->options_.set_refresh_factor(info.refresh_factor);

  // TODO [tastolfi 2021-12-10] kMaxPageRefDepth vs info.max_page_ref_depth?

  return OkStatus();
}

}  // namespace llfs
