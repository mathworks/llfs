//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/mem_inode.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemInode::MemInode(fuse_ino_t ino, Category category, int mode) noexcept
    : state_{ino, category, mode}
{
  if (category == Category::kDirectory) {
    this->init_directory();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemInode::State::State(fuse_ino_t ino, Category category, int mode) noexcept
{
  (void)category;
  std::memset(&this->entry_, 0, sizeof(this->entry_));
  this->entry_.ino = ino;
  this->entry_.attr.st_ino = ino;
  this->entry_.attr.st_mode = (mode_t)category | mode;
  this->entry_.attr.st_uid = 1001;  // TODO [tastolfi 2023-07-12] use arg
  this->entry_.attr.st_gid = 1001;  // TODO [tastolfi 2023-07-12] use arg
  this->entry_.attr.st_blksize = 4096;
  this->entry_.attr.st_blocks = 8;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemInode::init_directory()
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::State::find_child_by_name(const std::string& child_name)
    -> batt::StatusOr<batt::SharedPtr<MemInode>>
{
  auto child_iter = this->children_by_name_.find(child_name);
  if (child_iter == this->children_by_name_.end()) {
    return {batt::status_from_errno(ENOENT)};
  }
  return child_iter->second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::lookup_child(const std::string& name) -> batt::StatusOr<const fuse_entry_param*>
{
  auto locked = this->state_.lock();

  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> child_inode, locked->find_child_by_name(name));

  // Check to see whether the child inode is being deleted.
  //
  if (child_inode->is_dead()) {
    return {batt::status_from_errno(ENOENT)};
  }

  // Increment the lookup count.
  //
  child_inode->add_lookup(1);

  return {&child_inode->state_.lock()->entry_};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::get_attributes() -> batt::StatusOr<FuseImplBase::Attributes>
{
  return {FuseImplBase::Attributes{
      .attr = &this->state_.lock()->entry_.attr,
      .timeout_sec = 1.0,
  }};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<FuseImplBase::Attributes> MemInode::set_attributes(const struct stat* attr,
                                                                  int to_set)
{
  auto locked = this->state_.lock();

  if ((to_set & FUSE_SET_ATTR_MODE) != 0) {
    locked->entry_.attr.st_mode = attr->st_mode;
  }

  if ((to_set & FUSE_SET_ATTR_UID) != 0) {
    locked->entry_.attr.st_uid = attr->st_uid;
  }

  if ((to_set & FUSE_SET_ATTR_GID) != 0) {
    locked->entry_.attr.st_gid = attr->st_gid;
  }

  if ((to_set & FUSE_SET_ATTR_SIZE) != 0) {
    const u64 old_size = locked->entry_.attr.st_size;
    const u64 new_size = attr->st_size;
    const bool truncated = (new_size < old_size);

    locked->entry_.attr.st_size = new_size;

    if (truncated) {
      auto iter = locked->data_blocks_.lower_bound(new_size);

      // Adjust iter, in case lower_bound overshot (we want the greatest lower bound).
      //
      if (iter != locked->data_blocks_.begin() &&
          (iter == locked->data_blocks_.end() || iter->first > new_size)) {
        --iter;
      }

      if (iter != locked->data_blocks_.end()) {
        const u64 block_pos = iter->first;
        if (iter->first < BATT_CHECKED_CAST(u64, attr->st_size)) {
          // Clear out the truncated portion of the new last block.
          //
          const u64 block_offset = new_size - block_pos;
          const usize n_to_clear =
              std::min(iter->second->size() - block_offset, old_size - new_size);

          LLFS_VLOG(1) << BATT_INSPECT(block_offset) << BATT_INSPECT(n_to_clear)
                       << BATT_INSPECT(iter->second->size() - block_offset)
                       << BATT_INSPECT(old_size - new_size);

          std::memset(iter->second->data() + block_offset, 0, n_to_clear);

          // Move to iter to the next block before erasing.
          //
          ++iter;
        }
        locked->data_blocks_.erase(iter, locked->data_blocks_.end());
      }
    }
  }

  if ((to_set & FUSE_SET_ATTR_ATIME) != 0) {
    locked->entry_.attr.st_atime = attr->st_atime;
  }

  if ((to_set & FUSE_SET_ATTR_MTIME) != 0) {
    locked->entry_.attr.st_mtime = attr->st_mtime;
  }

  if ((to_set & FUSE_SET_ATTR_ATIME_NOW) != 0) {
    BATT_REQUIRE_OK(
        batt::status_from_retval(clock_gettime(CLOCK_REALTIME, &locked->entry_.attr.st_atim)));
  }

  if ((to_set & FUSE_SET_ATTR_MTIME_NOW) != 0) {
    BATT_REQUIRE_OK(
        batt::status_from_retval(clock_gettime(CLOCK_REALTIME, &locked->entry_.attr.st_mtim)));
  }

  if ((to_set & FUSE_SET_ATTR_CTIME) != 0) {
    locked->entry_.attr.st_ctime = attr->st_ctime;
  }

  return {FuseImplBase::Attributes{
      .attr = &locked->entry_.attr,
      .timeout_sec = 1.0,
  }};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::State::pack_as_fuse_dir_entry(fuse_req_t req, batt::ConstBuffer& out_buf,
                                             batt::MutableBuffer& dst_buf, const std::string& name,
                                             DirentOffset next_offset, PlusApi plus_api) const
    -> batt::Status
{
  // TODO [tastolfi 2023-06-30] do we need to bump the inode refcount here?
  //
  usize size_needed = [&] {
    LLFS_VLOG(1) << "DEBUG: readdir(): added to buffer: " << batt::c_str_literal(name) << ", ino "
                 << this->entry_.attr.st_ino << ", next_offset " << next_offset;

    if (plus_api) {
      return fuse_add_direntry_plus(req, static_cast<char*>(dst_buf.data()), dst_buf.size(),
                                    name.c_str(), &this->entry_, next_offset);
    } else {
      return fuse_add_direntry(req, static_cast<char*>(dst_buf.data()), dst_buf.size(),
                               name.c_str(), &this->entry_.attr, next_offset);
    }
  }();

  if (size_needed > dst_buf.size()) {
    return {batt::StatusCode::kResourceExhausted};
  }

  out_buf = batt::ConstBuffer{out_buf.data(), out_buf.size() + size_needed};
  dst_buf += size_needed;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::readdir(fuse_req_t req, MemFileHandle& /*dh*/, size_t size, DirentOffset offset,
                       PlusApi plus_api)  //
    -> batt::StatusOr<FuseReadDirData>
{
  LLFS_VLOG(1) << "DEBUG: MemInode::readdir(): current offset " << offset;

  std::unique_ptr<char[]> storage{new (std::nothrow) char[size]};
  if (!storage) {
    return {batt::status_from_errno(ENOMEM)};
  }

  batt::ConstBuffer out_buf{storage.get(), 0u};
  batt::MutableBuffer dst_buf{storage.get(), size};

  {
    auto locked = this->state_.lock();

    auto children_slice = batt::as_slice(locked->children_by_offset_);
    children_slice.advance_begin(std::min<isize>(offset, children_slice.size()));

    for (const auto& [child_inode, name] : children_slice) {
      const auto next_offset = DirentOffset{offset + 1};

      //----- --- -- -  -  -   -
      // TODO [tastolfi 2023-06-30] maybe do something like this?
      //
      // e.attr.st_ino = entry->d_ino;
      // e.attr.st_mode = entry->d_type << 12;
      //----- --- -- -  -  -   -

      batt::Status pack_status = child_inode->state_.lock()->pack_as_fuse_dir_entry(
          req, out_buf, dst_buf, name.c_str(), next_offset, plus_api);

      if (pack_status == batt::StatusCode::kResourceExhausted) {
        break;
      }

      /* In contrast to readdir() (which does not affect the lookup counts),
       * the lookup count of every entry returned by readdirplus(), except "."
       * and "..", is incremented by one.
       *
       *   - libfuse/include/fuse_lowlevel.h
       */
      if (plus_api) {
        child_inode->add_lookup(1);
      }

      BATT_REQUIRE_OK(pack_status);

      offset = next_offset;
    }
  }

  LLFS_VLOG(1) << "MemInode::readdir() returning buffer size: " << out_buf.size()
               << " (max req=" << size << ")";

  return {FuseReadDirData{FuseImplBase::OwnedConstBuffer{std::move(storage), out_buf}}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::add_child(const std::string& name, batt::SharedPtr<MemInode>&& child_inode)
    -> batt::Status
{
  BATT_REQUIRE_OK(this->acquire_count_lock());
  auto on_scope_exit = batt::finally([&] {
    this->release_count_lock();
  });
  {
    auto locked = this->state_.lock();

    BATT_REQUIRE_OK(child_inode->increment_link_refs(1));

    locked->children_by_name_.emplace(name, child_inode);
    locked->children_by_offset_.emplace_back(std::move(child_inode), name);

    return batt::OkStatus();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::remove_child(const std::string& name, IsDir is_dir, RequireEmpty require_empty)
    -> batt::StatusOr<std::pair<IsDead, batt::SharedPtr<MemInode>>>
{
  auto locked = this->state_.lock();

  auto iter = locked->children_by_name_.find(name);
  if (iter == locked->children_by_name_.end()) {
    return batt::status_from_errno(ENOENT);
  }

  auto iter2 = std::find_if(locked->children_by_offset_.begin(), locked->children_by_offset_.end(),
                            [iter](const std::pair<batt::SharedPtr<MemInode>, std::string>& entry) {
                              return entry.first == iter->second;
                            });
  if (iter2 == locked->children_by_offset_.end()) {
    return batt::status_from_errno(EIO);
  }

  batt::SharedPtr<MemInode> child_inode = iter->second;

  if (child_inode->is_dir() != is_dir) {
    return batt::status_from_errno(EINVAL);
  }

  if (require_empty && child_inode->is_dir() && !child_inode->is_empty()) {
    return batt::status_from_errno(ENOTEMPTY);
  }

  BATT_ASSIGN_OK_RESULT(const IsDead is_dead, child_inode->decrement_link_refs(1, require_empty));

  locked->children_by_name_.erase(iter);
  locked->children_by_offset_.erase(iter2);

  return {std::make_pair(is_dead, child_inode)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemInode::is_dir() const noexcept -> IsDir
{
  return IsDir{static_cast<MemInode::Category>(this->state_.lock()->entry_.attr.st_mode & S_IFMT) ==
               MemInode::Category::kDirectory};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemInode::write(u64 offset, const batt::Slice<const batt::ConstBuffer>& buffers)
{
  auto locked = this->state_.lock();

  const usize begin_offset = offset;
  for (const batt::ConstBuffer& buffer : buffers) {
    locked->write_chunk(offset, buffer);
    offset += buffer.size();
  }

  // Update the file size.
  //
  const auto written_upper_bound = BATT_CHECKED_CAST(i64, offset);
  if (written_upper_bound > locked->file_size()) {
    locked->file_size(written_upper_bound);
  }

  return offset - begin_offset;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemInode::State::write_chunk(u64 offset, batt::ConstBuffer buffer)
{
  const u64 first_block_pos = batt::round_down_bits(MemInode::kBlockBufferSizeLog2, offset);

  for (u64 block_pos = first_block_pos; buffer.size() > 0;
       block_pos += MemInode::kBlockBufferSize) {
    const usize block_offset = offset - block_pos;

    BATT_CHECK_GE(offset, block_pos);

    auto& p_block_buf = this->data_blocks_[block_pos];
    if (!p_block_buf) {
      p_block_buf = std::make_shared<BlockBuffer>();
      std::memset(p_block_buf->data(), 0, p_block_buf->size());
    }

    usize n_to_copy = std::min(buffer.size(), p_block_buf->size() - block_offset);
    std::memcpy(p_block_buf->data() + block_offset, buffer.data(), n_to_copy);

    buffer += n_to_copy;
    offset += n_to_copy;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FuseImplBase::WithCleanup<batt::Slice<batt::ConstBuffer>> MemInode::read(u64 offset, usize count)
{
  std::vector<std::shared_ptr<BlockBuffer>> blocks;
  std::vector<batt::ConstBuffer> buffers;

  {
    auto locked = this->state_.lock();

    while (count > 0) {
      std::shared_ptr<BlockBuffer> p_block;

      batt::ConstBuffer chunk = locked->read_chunk(offset, count, &p_block);
      if (chunk.size() == 0) {
        break;
      }
      buffers.emplace_back(chunk);

      offset += buffers.back().size();
      count -= buffers.back().size();
    }
  }

  auto buffers_slice = batt::as_slice(buffers);
  return FuseImplBase::with_cleanup(
      buffers_slice, [buffers = std::move(buffers), blocks = std::move(blocks)](auto&&...) {
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::ConstBuffer MemInode::State::read_chunk(u64 offset, usize count,
                                              std::shared_ptr<BlockBuffer>* p_block_out)
{
  static BlockBuffer zero_block;
  static const batt::ConstBuffer zero_buf = [] {
    std::memset(zero_block.data(), 0, zero_block.size());
    return batt::ConstBuffer{zero_block.data(), zero_block.size()};
  }();

  const u64 first_block_pos = batt::round_down_bits(MemInode::kBlockBufferSizeLog2, offset);
  const u64 first_block_offset = offset - first_block_pos;

  BATT_CHECK_GE(offset, first_block_pos);

  const batt::ConstBuffer data_block = [&] {
    auto iter = this->data_blocks_.find(first_block_pos);
    if (iter == this->data_blocks_.end()) {
      return zero_buf;
    }

    BATT_CHECK_NOT_NULLPTR(iter->second);

    if (p_block_out) {
      *p_block_out = iter->second;
    }

    return batt::ConstBuffer{
        iter->second->data(),
        iter->second->size(),
    };
  }();

  return batt::resize_buffer(data_block + first_block_offset,
                             std::min(count, this->file_size() - offset));
}

}  //namespace llfs
