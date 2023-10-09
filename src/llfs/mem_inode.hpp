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
#include <llfs/mem_inode_base.hpp>

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
class MemInode : public MemInodeBase<MemInode>
{
 public:
  static constexpr i32 kBlockBufferSizeLog2 = 12;
  static constexpr usize kBlockBufferSize = usize{1} << kBlockBufferSizeLog2;

  using BlockBuffer = std::array<char, kBlockBufferSize>;

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

  IsDir is_dir() const noexcept;

  bool is_empty() const noexcept
  {
    return this->state_.lock()->children_by_offset_.empty();
  }

  //----- --- -- -  -  -   -

  batt::StatusOr<FuseImplBase::Attributes> get_attributes();

  batt::StatusOr<FuseImplBase::Attributes> set_attributes(const struct stat* attr, int to_set);

  batt::StatusOr<FuseReadDirData> readdir(fuse_req_t req, MemFileHandle& dh, size_t size,
                                          DirentOffset offset, PlusApi plus_api);

  usize write(u64 offset, const batt::Slice<const batt::ConstBuffer>& buffers);

  FuseImplBase::WithCleanup<batt::Slice<batt::ConstBuffer>> read(u64 offset, usize count);

  //----- --- -- -  -  -   -
 private:
  void init_directory();

  //----- --- -- -  -  -   -
  struct State {
    fuse_entry_param entry_;

    std::unordered_map<std::string, batt::SharedPtr<MemInode>> children_by_name_;

    std::vector<std::pair<batt::SharedPtr<MemInode>, std::string>> children_by_offset_;

    std::map<u64, std::shared_ptr<BlockBuffer>> data_blocks_;

    //----- --- -- -  -  -   -

    State(fuse_ino_t ino, Category category, int mode) noexcept;

    batt::StatusOr<batt::SharedPtr<MemInode>> find_child_by_name(const std::string& child_name);

    batt::Status pack_as_fuse_dir_entry(fuse_req_t req, batt::ConstBuffer& out_buf,
                                        batt::MutableBuffer& dst_buf, const std::string& name,
                                        DirentOffset offset, PlusApi plus_api) const;

    void write_chunk(u64 offset, batt::ConstBuffer buffer);

    batt::ConstBuffer read_chunk(u64 offset, usize count,
                                 std::shared_ptr<BlockBuffer>* p_block_out);

    i64 file_size() const
    {
      return this->entry_.attr.st_size;
    }

    void file_size(i64 new_size)
    {
      this->entry_.attr.st_size = BATT_CHECKED_CAST(off_t, new_size);
    }
  };

  batt::Mutex<State> state_;

  /** \brief See state flags above.
   */
  batt::Watch<u64> count_{0};
};

}  //namespace llfs

#endif  // LLFS_MEM_INODE_HPP
