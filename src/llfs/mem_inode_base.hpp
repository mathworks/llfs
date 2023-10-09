//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEM_INODE_BASE_HPP
#define LLFS_MEM_INODE_BASE_HPP

#include <llfs/fuse.hpp>

#include <batteries/async/task.hpp>
#include <batteries/async/watch.hpp>

#include <batteries/shared_ptr.hpp>
#include <batteries/status.hpp>
#include <batteries/strong_typedef.hpp>

namespace llfs {

template <typename Derived>
class MemInodeBase : public batt::RefCounted<Derived>
{
 public:
  using FuseReadDirData = FuseImplBase::FuseReadDirData;

  BATT_STRONG_TYPEDEF(bool, RequireEmpty);
  BATT_STRONG_TYPEDEF(bool, IsDead);
  BATT_STRONG_TYPEDEF(bool, IsDir);

  enum struct Category : mode_t {
    kBlockSpecial = S_IFBLK,
    kCharSpecial = S_IFCHR,
    kFifoSpecial = S_IFIFO,
    kRegularFile = S_IFREG,
    kDirectory = S_IFDIR,
    kSymbolicLink = S_IFLNK,
  };

  static constexpr u64 kLookupCountShift = 0;
  static constexpr u64 kLinkCountShift = 40;
  static constexpr u64 kLockFlag = u64{1} << 63;
  static constexpr u64 kDeadFlag = u64{1} << 62;
  //
  static_assert(kLinkCountShift > kLookupCountShift);
  static_assert(kLookupCountShift == 0);
  //
  static constexpr u64 kLookupCountIncrement = u64{1} << kLookupCountShift;
  static constexpr u64 kLinkCountIncrement = u64{1} << kLinkCountShift;
  static constexpr u64 kMaxLookupCount = (u64{1} << kLinkCountShift) - 1;
  static constexpr u64 kMaxLinkCount = (u64{1} << (62 - kLinkCountShift)) - 1;
  static constexpr u64 kLookupCountMask = kMaxLookupCount;
  static constexpr u64 kLinkCountMask = kMaxLinkCount;

  static u64 get_lookup_count(u64 count) noexcept
  {
    return (count >> MemInodeBase::kLookupCountShift) & kLookupCountMask;
  }

  static u64 get_link_count(u64 count) noexcept
  {
    return (count >> MemInodeBase::kLinkCountShift) & kLinkCountMask;
  }

  static IsDead is_dead_state(u64 count) noexcept
  {
    return IsDead{(count & MemInodeBase::kDeadFlag) != 0};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void add_lookup(usize count) noexcept
  {
    const u64 increment = MemInodeBase::kLookupCountIncrement * count;
    const u64 prior_value = this->count_.fetch_add(increment);

    BATT_CHECK_LT(get_lookup_count(prior_value), kMaxLookupCount);
    BATT_CHECK_LT(get_lookup_count(prior_value), get_lookup_count(prior_value + increment));
  }

  IsDead forget(u64 count)
  {
    return this->remove_lookup(count);
  }

  IsDead is_dead() const noexcept
  {
    return is_dead_state(this->count_.get_value());
  }

  IsDead remove_lookup(usize count) noexcept;

  batt::Status increment_link_refs(usize count) noexcept;

  batt::StatusOr<IsDead> decrement_link_refs(usize count, RequireEmpty require_empty) noexcept;

  batt::Status acquire_count_lock() noexcept;

  void release_count_lock() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief See state flags above.
   */
  batt::Watch<u64> count_{0};
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Derived>
inline auto MemInodeBase<Derived>::remove_lookup(usize count) noexcept -> IsDead
{
  const u64 increment = MemInodeBase::kLookupCountIncrement * count;
  const u64 prior_value = this->count_.fetch_sub(increment);

  BATT_CHECK_GE(MemInodeBase::get_lookup_count(prior_value), count)
      << BATT_INSPECT(prior_value) << BATT_INSPECT(count) << BATT_INSPECT(increment)
      << BATT_INSPECT(std::bitset<64>{kLookupCountMask}) << BATT_INSPECT(kLookupCountShift);

  BATT_CHECK_GT(MemInodeBase::get_lookup_count(prior_value),
                MemInodeBase::get_lookup_count(prior_value - increment))
      << BATT_INSPECT(prior_value) << BATT_INSPECT(count) << BATT_INSPECT(increment);

  return MemInodeBase::is_dead_state(prior_value - increment);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Derived>
inline batt::Status MemInodeBase<Derived>::increment_link_refs(usize count) noexcept
{
  const u64 increment = MemInodeBase::kLinkCountIncrement * count;
  const u64 prior_value = this->count_.fetch_add(increment);

  BATT_CHECK_LT(MemInodeBase::get_link_count(prior_value), MemInodeBase::kMaxLinkCount);
  BATT_CHECK_LT(MemInodeBase::get_link_count(prior_value),
                MemInodeBase::get_link_count(prior_value + increment));

  if ((prior_value & MemInodeBase::kDeadFlag) != 0) {
    this->count_.fetch_sub(increment);
    return batt::status_from_errno(ENOENT);
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Derived>
inline auto MemInodeBase<Derived>::decrement_link_refs(usize count,
                                                       RequireEmpty require_empty) noexcept
    -> batt::StatusOr<IsDead>
{
  if (count == 0) {
    return {batt::status_from_errno(EINVAL)};
  }

  const u64 increment = MemInodeBase::kLinkCountIncrement * count;

  BATT_REQUIRE_OK(this->acquire_count_lock());
  auto on_scope_exit = batt::finally([&] {
    this->release_count_lock();
  });

  const batt::Optional<u64> updated_value =
      this->count_.modify_if([&](u64 observed_value) -> batt::Optional<u64> {
        const u64 observed_link_count = MemInodeBase::get_link_count(observed_value);
        const bool will_be_dead = ((observed_link_count - count) == 0);

        BATT_CHECK_LE(count, observed_link_count);
        BATT_CHECK_GE(MemInodeBase::get_link_count(observed_value),
                      MemInodeBase::get_link_count(observed_value - increment))
            << "Integer wrap!";

        // If the requested decrement would bring the link count to zero but we are not empty, this
        // is an error.
        //
        if (require_empty && will_be_dead && !static_cast<Derived*>(this)->is_empty()) {
          return batt::None;
        }

        // Everything looks good!  Attempt to CAS-modify the count.
        //
        u64 target_value = observed_value - increment;
        if (will_be_dead) {
          target_value |= MemInodeBase::kDeadFlag;
        }

        return target_value;
      });

  if (!updated_value) {
    return {batt::status_from_errno(ENOTEMPTY)};
  }

  return MemInodeBase::is_dead_state(*updated_value);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Derived>
inline batt::Status MemInodeBase<Derived>::acquire_count_lock() noexcept
{
  for (;;) {
    const u64 prior_value = this->count_.fetch_or(MemInodeBase::kLockFlag);
    if (!(prior_value & MemInodeBase::kLockFlag)) {
      break;
    }
    if ((prior_value & MemInodeBase::kDeadFlag)) {
      return {batt::status_from_errno(ENOENT)};
    }
    batt::Task::yield();
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Derived>
inline void MemInodeBase<Derived>::release_count_lock() noexcept
{
  this->count_.fetch_and(~MemInodeBase::kLockFlag);
}

}  //namespace llfs

#endif  // LLFS_MEM_INODE_BASE_HPP
