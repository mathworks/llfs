//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs_fuse/mem_fuse.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemoryFuseImpl::MemInode::MemInode(fuse_ino_t ino, Category category) noexcept
    : state_{ino, category}
{
  if (category == Category::kDirectory) {
    this->init_directory();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemoryFuseImpl::MemInode::State::State(fuse_ino_t ino, Category category) noexcept
{
  (void)category;
  std::memset(&this->entry_, 0, sizeof(this->entry_));
  this->entry_.ino = ino;
  this->entry_.attr.st_ino = ino;
  this->entry_.attr.st_mode = (mode_t)category | 0755;
  this->entry_.attr.st_uid = 1001;
  this->entry_.attr.st_gid = 1001;
  this->entry_.attr.st_blksize = 4096;
  this->entry_.attr.st_blocks = 8;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryFuseImpl::MemInode::init_directory()
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemoryFuseImpl::MemInode::State::find_child_by_name(const std::string& child_name)
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
auto MemoryFuseImpl::MemInode::lookup(const std::string& name)
    -> batt::StatusOr<const fuse_entry_param*>
{
  auto locked = this->state_.lock();

  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> child_inode, locked->find_child_by_name(name));

  // Increment the lookup count.
  //
  child_inode->add_lookup();

  return {&child_inode->state_.lock()->entry_};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemoryFuseImpl::MemInode::forget(u64 count) -> IsDead
{
  return this->remove_lookup(count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemoryFuseImpl::MemInode::add_lookup(usize count) noexcept
{
  this->count_.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemoryFuseImpl::MemInode::remove_lookup(usize count) noexcept -> IsDead
{
  const u64 prior_count = this->count_.fetch_sub(count);
  const u64 prior_lookup_count = prior_count & MemInode::kLookupCountMask;

  BATT_CHECK_GE(prior_lookup_count, count);

  return IsDead{prior_lookup_count - count == 0 &&
                (MemInode::kUnlinkedFlag & prior_count) == kUnlinkedFlag};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemoryFuseImpl::MemInode::get_attributes(fuse_req_t /*req*/, fuse_file_info* /*fi*/)
    -> batt::StatusOr<FuseImplBase::Attributes>
{
  return {FuseImplBase::Attributes{
      .attr = &this->state_.lock()->entry_.attr,
      .timeout_sec = 1.0,
  }};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<FuseImplBase::Attributes> MemoryFuseImpl::MemInode::set_attributes(
    fuse_req_t req, fuse_file_info* fi, struct stat* attr, int to_set)
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
    locked->entry_.attr.st_size = attr->st_size;
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

  *attr = locked->entry_.attr;

  return {FuseImplBase::Attributes{
      .attr = attr,
      .timeout_sec = 1.0,
  }};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemoryFuseImpl::MemInode::State::pack_as_fuse_dir_entry(
    fuse_req_t req, batt::ConstBuffer& out_buf, batt::MutableBuffer& dst_buf,
    const std::string& name, DirentOffset next_offset, PlusApi plus_api) const -> batt::Status
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
auto MemoryFuseImpl::MemInode::readdir(MemFileHandle& dh, fuse_req_t req, size_t size,
                                       DirentOffset offset, fuse_file_info* fi,
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
      BATT_REQUIRE_OK(pack_status);

      offset = next_offset;
    }
  }

  LLFS_VLOG(1) << "MemInode::readdir() returning buffer size: " << out_buf.size()
               << " (max req=" << size << ")";

  return {FuseReadDirData{OwnedConstBuffer{std::move(storage), out_buf}}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemoryFuseImpl::MemInode::add_child_entry(fuse_req_t req, const std::string& name,
                                               batt::SharedPtr<MemInode>&& child_inode)
    -> batt::Status
{
  auto locked = this->state_.lock();

  locked->children_by_name_.emplace(name, child_inode);
  locked->children_by_offset_.emplace_back(std::move(child_inode), name);

  return batt::OkStatus();
}

}  //namespace llfs
