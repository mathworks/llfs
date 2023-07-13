//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEM_INODE_HPP
#define LLFS_MEM_INODE_HPP

#include <llfs/fuse.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>

#include <batteries/buffer.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/status.hpp>
#include <batteries/strong_typedef.hpp>

#include <string>
#include <unordered_map>
#include <vector>

namespace llfs {

class MemFileHandle;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class MemInode : public batt::RefCounted<MemInode>
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

  // for this->count_ -- TODO [tastolfi 2023-07-11] we should just keep a unified ref_count Watch
  // in this class, and modify that from all of add_child_entry, unlink, lookup, and forget.
  //
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
    return (count >> MemInode::kLookupCountShift) & MemInode::kLookupCountMask;
  }

  static u64 get_link_count(u64 count) noexcept
  {
    return (count >> MemInode::kLinkCountShift) & MemInode::kLinkCountMask;
  }

  static IsDead is_dead_state(u64 count) noexcept
  {
    return IsDead{(count & MemInode::kDeadFlag) != 0};
  }

  //----- --- -- -  -  -   -

  explicit MemInode(fuse_ino_t ino, Category category, int mode) noexcept;

  //----- --- -- -  -  -   -

  const fuse_entry_param* get_fuse_entry_param() const noexcept
  {
    auto locked = this->state_.lock();
    return &locked->entry_;
  }

  fuse_ino_t get_ino() const noexcept
  {
    return this->state_.lock()->entry_.ino;
  }

  batt::Status add_child(const std::string& name, batt::SharedPtr<MemInode>&& child_inode);

  batt::StatusOr<std::pair<IsDead, batt::SharedPtr<MemInode>>> remove_child(
      const std::string& name, IsDir is_dir, RequireEmpty require_empty);

  //----- --- -- -  -  -   -

  batt::StatusOr<const fuse_entry_param*> lookup_child(const std::string& name);

  void add_lookup(usize count) noexcept;

  IsDead forget(u64 count);

  IsDir is_dir() const noexcept;

  bool is_empty() const noexcept
  {
    return this->state_.lock()->children_by_offset_.empty();
  }

  IsDead is_dead() const noexcept
  {
    return MemInode::is_dead_state(this->count_.get_value());
  }

  //----- --- -- -  -  -   -

  batt::StatusOr<FuseImplBase::Attributes> get_attributes();

  batt::StatusOr<FuseImplBase::Attributes> set_attributes(const struct stat* attr, int to_set);

  batt::StatusOr<FuseReadDirData> readdir(fuse_req_t req, MemFileHandle& dh, size_t size,
                                          DirentOffset offset, PlusApi plus_api);

  //----- --- -- -  -  -   -
 private:
  IsDead remove_lookup(usize count) noexcept;

  batt::Status increment_link_refs(usize count) noexcept;

  batt::StatusOr<IsDead> decrement_link_refs(usize count, RequireEmpty require_empty) noexcept;

  void init_directory();

  batt::Status acquire_count_lock() noexcept;

  void release_count_lock() noexcept;

  //----- --- -- -  -  -   -
  struct State {
    fuse_entry_param entry_;
    std::unordered_map<std::string, batt::SharedPtr<MemInode>> children_by_name_;
    std::vector<std::pair<batt::SharedPtr<MemInode>, std::string>> children_by_offset_;

    State(fuse_ino_t ino, Category category, int mode) noexcept;

    batt::StatusOr<batt::SharedPtr<MemInode>> find_child_by_name(const std::string& child_name);

    batt::Status pack_as_fuse_dir_entry(fuse_req_t req, batt::ConstBuffer& out_buf,
                                        batt::MutableBuffer& dst_buf, const std::string& name,
                                        DirentOffset offset, PlusApi plus_api) const;
  };

  batt::Mutex<State> state_;

  /** \brief See state flags above.
   */
  batt::Watch<u64> count_{0};
};

}  //namespace llfs

#endif  // LLFS_MEM_INODE_HPP
