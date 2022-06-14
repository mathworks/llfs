#pragma once
#ifndef LLFS_SLOT_READ_LOCK_HPP
#define LLFS_SLOT_READ_LOCK_HPP

#include <llfs/pointers.hpp>
#include <llfs/slot.hpp>

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#if __GNUC__ >= 9
#pragma GCC diagnostic ignored "-Wdeprecated-copy"
#endif  // __GNUC__ >= 9
#endif  // __GNUC__

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
    if (this->sponsor_) {
      this->sponsor_->unlock_slots(this);
    }
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

  SlotReadLock(SlotReadLock&&) = default;
  SlotReadLock& operator=(SlotReadLock&&) = default;

 private:
  UniqueNonOwningPtr<Sponsor> sponsor_;
  SlotRange range_{0, 0};
  SlotLockHeap::handle_type handle_;
  bool upper_bound_updated_;
};

}  // namespace llfs

#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif  // __GNUC__

#endif  // LLFS_SLOT_READ_LOCK_HPP
