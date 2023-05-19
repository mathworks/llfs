//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLOT_READ_LOCK_HPP
#define LLFS_SLOT_READ_LOCK_HPP

#include <llfs/pointers.hpp>
#include <llfs/slot.hpp>

#include <batteries/suppress.hpp>

BATT_SUPPRESS_IF_GCC("-Wunused-parameter")
BATT_SUPPRESS_IF_GCC("-Wdeprecated-copy")

#include <batteries/hint.hpp>

#include <batteries/hint.hpp>

#include <boost/heap/d_ary_heap.hpp>
#include <boost/heap/policies.hpp>

#include <memory>

namespace llfs {

struct SlotLockRecord {
  slot_offset_type slot_offset;
  const char* holder;
};

inline std::ostream& operator<<(std::ostream& out, const SlotLockRecord& t)
{
  return out << "SlotLockRecord{.slot_offset=" << t.slot_offset
             << ", .holder=" << batt::c_str_literal(t.holder) << ",}";
}

inline slot_offset_type get_slot_offset(const SlotLockRecord& rec)
{
  return rec.slot_offset;
}

using SlotLockHeap = boost::heap::d_ary_heap<SlotLockRecord,                            //
                                             boost::heap::arity<2>,                     //
                                             boost::heap::compare<SlotOffsetPriority>,  //
                                             boost::heap::mutable_<true>                //
                                             >;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SlotReadLock
{
 public:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class Sponsor
  {
    friend class SlotReadLock;

   public:
    Sponsor(const Sponsor&) = delete;
    Sponsor& operator=(const Sponsor&) = delete;

    virtual ~Sponsor() = default;

   protected:
    Sponsor() = default;

   private:
    virtual void unlock_slots(SlotReadLock* read_lock) = 0;

    virtual SlotReadLock clone_lock(const SlotReadLock* read_lock) = 0;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static SlotReadLock null_lock(const SlotRange& slot_range = SlotRange{0, 0})
  {
    class NullSponsor : public Sponsor
    {
     public:
      void unlock_slots(SlotReadLock*) override
      {
      }

      SlotReadLock clone_lock(const SlotReadLock* lock) override
      {
        return SlotReadLock{/*sponsor=*/this, lock->range_, lock->handle_,
                            lock->upper_bound_updated_};
      }
    };

    static NullSponsor null_sponsor_;

    return SlotReadLock{&null_sponsor_, slot_range, SlotLockHeap::handle_type{}};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  SlotReadLock() = default;

  explicit SlotReadLock(Sponsor* sponsor, const SlotRange& range,
                        const SlotLockHeap::handle_type& handle,
                        bool upper_bound_updated = false) noexcept
      : sponsor_{sponsor}
      , range_{range}
      , handle_{handle}
      , upper_bound_updated_{upper_bound_updated}
  {
  }

  ~SlotReadLock() noexcept
  {
    this->clear();
  }

  void clear()
  {
    if (this->sponsor_) {
      this->sponsor_->unlock_slots(this);
    }
    this->sponsor_ = nullptr;
  }

  Sponsor* get_sponsor() const
  {
    return this->sponsor_.get();
  }

  const SlotRange& slot_range() const
  {
    return this->range_;
  }

  bool is_upper_bound_updated() const
  {
    return this->upper_bound_updated_;
  }

  [[nodiscard]] SlotLockHeap::handle_type release()
  {
    this->sponsor_ = nullptr;
    return this->handle_;
  }

  SlotReadLock clone() const
  {
    if (this->sponsor_ == nullptr) {
      return SlotReadLock{};
    }
    return this->sponsor_->clone_lock(this);
  }

  explicit operator bool() const
  {
    return this->sponsor_ != nullptr;
  }

  SlotReadLock(const SlotReadLock&) = delete;
  SlotReadLock& operator=(const SlotReadLock&) = delete;

  SlotReadLock(SlotReadLock&& other) noexcept
      : sponsor_{std::move(other.sponsor_)}
      , range_{std::move(other.range_)}
      , handle_{std::move(other.handle_)}
      , upper_bound_updated_{other.upper_bound_updated_}
  {
    other.range_ = SlotRange{};
    other.handle_ = SlotLockHeap::handle_type{};
    other.upper_bound_updated_ = false;
  }

  SlotReadLock& operator=(SlotReadLock&& other) noexcept
  {
    if (BATT_HINT_TRUE(this != &other)) {
      this->clear();
      this->sponsor_ = std::move(other.sponsor_);
      this->range_ = std::move(other.range_);
      this->handle_ = std::move(other.handle_);
      this->upper_bound_updated_ = other.upper_bound_updated_;

      other.range_ = SlotRange{};
      other.handle_ = SlotLockHeap::handle_type{};
      other.upper_bound_updated_ = false;
    }
    return *this;
  }

 private:
  UniqueNonOwningPtr<Sponsor> sponsor_;
  SlotRange range_{0, 0};
  SlotLockHeap::handle_type handle_;
  bool upper_bound_updated_;
};

}  // namespace llfs

BATT_UNSUPPRESS_IF_GCC()  // "-Wdeprecated-copy"
BATT_UNSUPPRESS_IF_GCC()  // "-Wunused-parameter"

#endif  // LLFS_SLOT_READ_LOCK_HPP
