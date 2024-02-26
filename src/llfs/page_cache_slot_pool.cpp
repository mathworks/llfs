//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_slot.hpp>
//

#include <random>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageCacheSlot::Pool::Pool(usize n_slots, std::string&& name,
                                       usize eviction_candidates) noexcept
    : n_slots_{n_slots}
    , eviction_candidates_{std::min<usize>(n_slots, std::max<usize>(2, eviction_candidates))}
    , name_{std::move(name)}
    , slot_storage_{new SlotStorage[n_slots]}
{
  this->metrics_.max_slots.set(n_slots);

  const auto metric_name = [this](std::string_view property) {
    return batt::to_string("Cache_", this->name_, "_", property);
  };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), this->metrics_.n)

  ADD_METRIC_(max_slots);
  ADD_METRIC_(indexed_slots);
  ADD_METRIC_(query_count);
  ADD_METRIC_(hit_count);
  ADD_METRIC_(stale_count);
  ADD_METRIC_(alloc_count);
  ADD_METRIC_(evict_count);
  ADD_METRIC_(insert_count);
  ADD_METRIC_(erase_count);
  ADD_METRIC_(full_count);

#undef ADD_METRIC_
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot::Pool::~Pool() noexcept
{
  if (this->slot_storage_) {
    const usize n_to_delete = this->n_constructed_.get_value();
    BATT_CHECK_EQ(n_to_delete, this->n_allocated_.load());

    for (usize i = 0; i < n_to_delete; ++i) {
      BATT_DEBUG_INFO("Destructing slot " << i << BATT_INSPECT(n_to_delete)
                                          << BATT_INSPECT(this->n_allocated_.load())
                                          << BATT_INSPECT(this->n_slots_));
      this->get_slot(i)->~PageCacheSlot();
    }
  }

  global_metric_registry()
      .remove(this->metrics_.max_slots)
      .remove(this->metrics_.indexed_slots)
      .remove(this->metrics_.query_count)
      .remove(this->metrics_.hit_count)
      .remove(this->metrics_.stale_count)
      .remove(this->metrics_.alloc_count)
      .remove(this->metrics_.evict_count)
      .remove(this->metrics_.insert_count)
      .remove(this->metrics_.erase_count)
      .remove(this->metrics_.full_count);

  LLFS_VLOG(1) << "PageCacheSlot::Pool::~Pool()";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot* PageCacheSlot::Pool::get_slot(usize i) noexcept
{
  BATT_CHECK_LT(i, this->n_slots_);

  return std::addressof(*this->slots()[i]);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot* PageCacheSlot::Pool::allocate() noexcept
{
  if (this->n_allocated_.load() < this->n_slots_) {
    const usize allocated_i = this->n_allocated_.fetch_add(1);
    if (allocated_i < this->n_slots_) {
      void* storage_addr = this->slots() + allocated_i;
      PageCacheSlot* const new_slot = new (storage_addr) PageCacheSlot{*this};
      this->n_constructed_.fetch_add(1);
      return new_slot;
    }
    const usize reverted = this->n_allocated_.fetch_sub(1);
    BATT_CHECK_GE(reverted, this->n_slots_);
    //
    // continue...
  }

  BATT_CHECK_OK(this->n_constructed_.await_equal(this->n_slots_));

  return this->evict_lru();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::Pool::index_of(const PageCacheSlot* slot) noexcept
{
  BATT_CHECK_NOT_NULLPTR(slot);
  BATT_CHECK_EQ(std::addressof(slot->pool()), this);

  const usize index = batt::CpuCacheLineIsolated<PageCacheSlot>::pointer_from(slot) - this->slots();

  BATT_CHECK_LT(index, this->n_slots_);

  return index;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::CpuCacheLineIsolated<PageCacheSlot>* PageCacheSlot::Pool::slots() noexcept
{
  return reinterpret_cast<batt::CpuCacheLineIsolated<PageCacheSlot>*>(this->slot_storage_.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot* PageCacheSlot::Pool::evict_lru()
{
  thread_local std::default_random_engine rng{/*seed=*/std::random_device{}()};

  const usize n_slots = this->n_constructed_.get_value();

  if (n_slots == 0) {
    return nullptr;
  }

  if (n_slots == 1) {
    PageCacheSlot* only_slot = this->get_slot(0);
    if (only_slot->evict()) {
      return only_slot;
    }
    return nullptr;
  }

  // Pick k slots at random and try to evict whichever one has the least (earliest) latest use
  // logical time stamp.
  //
  std::uniform_int_distribution<usize> pick_first_slot{0, n_slots - 1};
  std::uniform_int_distribution<usize> pick_second_slot{0, n_slots - 2};

  for (usize attempts = 0; attempts < n_slots; ++attempts) {
    usize first_slot_i = pick_first_slot(rng);
    usize second_slot_i = pick_second_slot(rng);

    if (second_slot_i >= first_slot_i) {
      ++second_slot_i;
    }
    BATT_CHECK_NE(first_slot_i, second_slot_i);

    PageCacheSlot* first_slot = this->get_slot(first_slot_i);
    PageCacheSlot* second_slot = this->get_slot(second_slot_i);
    PageCacheSlot* lru_slot = [&] {
      if (first_slot->get_latest_use() - second_slot->get_latest_use() < 0) {
        return first_slot;
      }
      return second_slot;
    }();

    // Pick more random slots (with replacement, since we already have >= 2) to try to get a better
    // (older) last-usage LTS.
    //
    for (usize k = 2; k < this->eviction_candidates_; ++k) {
      usize nth_slot_i = pick_first_slot(rng);
      PageCacheSlot* nth_slot = this->get_slot(nth_slot_i);
      lru_slot = [&] {
        if (nth_slot->get_latest_use() - lru_slot->get_latest_use() < 0) {
          return nth_slot;
        }
        return lru_slot;
      }();
    }

    // Fingers crossed!
    //
    if (lru_slot->evict()) {
      this->metrics_.evict_count.fetch_add(1);
      return lru_slot;
    }
  }

  return nullptr;
}

}  //namespace llfs
