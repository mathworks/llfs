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
    , latest_batch_{}
    , recycler_uuid_{boost::uuids::random_generator{}()}
    , latest_info_refresh_slot_{}
    , volume_trim_slot_{0}
    , largest_page_index_{0}
{
  initialize_status_codes();
  LLFS_VLOG(1) << "PageRecyclerRecoveryVisitor()";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageRecyclerRecoveryVisitor::~PageRecyclerRecoveryVisitor() noexcept
{
  LLFS_VLOG(1) << "~PageRecyclerRecoveryVisitor";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageRecyclerRecoveryVisitor::set_trim_pos(slot_offset_type trim_pos) noexcept
{
  this->trim_pos_ = trim_pos;
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

  to_recycle.reserve(this->recovered_pages_.size());
  for (const auto& [page_id, page_to_recycle] : this->recovered_pages_) {
    to_recycle.emplace_back(page_to_recycle);
  }

  // Remove all the deleted/prepared pages from the list.
  //
  to_recycle.erase(std::remove_if(to_recycle.begin(), to_recycle.end(),
                                  [](const PageToRecycle& p) {
                                    return p.batch_slot;
                                  }),
                   to_recycle.end());

  // Sort by slot offset.
  //
  std::sort(to_recycle.begin(), to_recycle.end(), SlotOffsetOrder{});

  return to_recycle;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Optional<PageRecycler::Batch>> PageRecyclerRecoveryVisitor::consume_latest_batch()
{
  LLFS_VLOG(1) << "consuming latest batch: " << this->latest_batch_;

  if (!this->latest_batch_) {
    return {None};
  }

  PageRecycler::Batch consumed_batch = std::move(*this->latest_batch_);
  this->latest_batch_ = None;

  return {std::move(consumed_batch)};
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
slot_offset_type PageRecyclerRecoveryVisitor::volume_trim_offset() const
{
  return this->volume_trim_slot_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u32 PageRecyclerRecoveryVisitor::page_index() const
{
  return this->largest_page_index_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageRecyclerRecoveryVisitor::operator()(const SlotParse& slot,
                                               const PageToRecycle& recovered)
{
  LLFS_VLOG(1) << "Recovered slot: " << slot.offset << " " << recovered
               << BATT_INSPECT((bool)this->latest_batch_) << BATT_INSPECT(this->volume_trim_slot_)
               << BATT_INSPECT(this->largest_page_index_);

  // Add the new record, or verify that the existing one and the new one are equivalent.
  //
  auto iter = this->recovered_pages_.find(recovered.page_id);
  if (iter == this->recovered_pages_.end()) {
    bool inserted = false;
    std::tie(iter, inserted) = this->recovered_pages_.emplace(recovered.page_id, recovered);
    BATT_CHECK(inserted);
  } else {
    PageToRecycle& existing_record = iter->second;

    BATT_CHECK_EQ(existing_record.depth, recovered.depth);
    BATT_CHECK(existing_record.refresh_slot &&
               !slot_less_than(slot.offset.lower_bound, *existing_record.refresh_slot))
        << BATT_INSPECT(existing_record) << BATT_INSPECT(recovered);
    if (existing_record.batch_slot) {
      BATT_CHECK(recovered.batch_slot);
      BATT_CHECK_EQ(existing_record.batch_slot, recovered.batch_slot);
    }

    existing_record.batch_slot = recovered.batch_slot;
  }

  // Save the largest unique offset identifier. Note that if offsets are same then we need to only
  // update the largest page_index.
  //
  if (this->volume_trim_slot_ < recovered.volume_trim_slot) {
    this->volume_trim_slot_ = recovered.volume_trim_slot;
    this->largest_page_index_ = recovered.page_index;
  } else if (this->volume_trim_slot_ == recovered.volume_trim_slot &&
             this->largest_page_index_ < recovered.page_index) {
    this->largest_page_index_ = recovered.page_index;
  }

  PageToRecycle& to_recycle = iter->second;
  to_recycle.refresh_slot = slot.offset.lower_bound;

  if (to_recycle.batch_slot) {
    if (this->latest_batch_) {
      if (this->latest_batch_->slot_offset != *to_recycle.batch_slot) {
        LLFS_LOG_WARNING() << "kTooManyPendingBatchesDuringRecovery" << BATT_INSPECT(to_recycle)
                           << BATT_INSPECT(*this->latest_batch_);
        return ::llfs::make_status(StatusCode::kTooManyPendingBatchesDuringRecovery);
      }
      if (this->latest_batch_->depth != to_recycle.depth) {
        LLFS_LOG_WARNING() << "Inconsistent batch depth;" << BATT_INSPECT(to_recycle)
                           << BATT_INSPECT(*this->latest_batch_);
        return batt::StatusCode::kInternal;  // TODO [tastolfi 2023-03-20]
      }
    } else {
      this->latest_batch_.emplace();
      this->latest_batch_->slot_offset = *to_recycle.batch_slot;
      this->latest_batch_->depth = to_recycle.depth;
      LLFS_VLOG(1) << " -- Starting batch: " << this->latest_batch_;
    }

    BATT_CHECK(this->latest_batch_.has_value());
    this->latest_batch_->to_recycle.emplace_back(to_recycle);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageRecyclerRecoveryVisitor::operator()(const SlotParse& slot,
                                               const PackedRecycleBatchCommit& commit)
{
  LLFS_VLOG(1) << "Recovered slot: " << slot.offset << " " << commit;

  // It's OK if the latest prepared batch has been trimmed off the log; but if we *can* see it,
  // then validate that the batch slots match here.
  //
  if (this->latest_batch_) {
    if (this->latest_batch_->slot_offset != commit.batch_slot) {
      LLFS_LOG_WARNING() << "kInvalidBatchCommitDuringRecovery: "
                         << BATT_INSPECT(*this->latest_batch_) << BATT_INSPECT(commit.batch_slot);
      return llfs::make_status(StatusCode::kInvalidBatchCommitDuringRecovery);
    }
  } else {
    if (slot_less_or_equal(this->trim_pos_, commit.batch_slot)) {
      LLFS_LOG_WARNING()
          << "Found PackedRecycleBatchCommit with no matching PackedRecyclePagePrepare(s)"
          << BATT_INSPECT(slot) << BATT_INSPECT(commit);
    }
  }
  LLFS_VLOG(1) << "Clearing" << BATT_INSPECT(this->latest_batch_);
  this->latest_batch_ = None;
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageRecyclerRecoveryVisitor::operator()(const SlotParse& slot,
                                               const PackedPageRecyclerInfo& info)
{
  LLFS_VLOG(1) << "Recovered slot: " << slot.offset << " " << info;

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
