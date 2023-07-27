//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/mem_fuse.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
fuse_ino_t MemoryFuseImpl::allocate_ino_int()
{
  return this->next_unused_ino_.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
FuseFileHandle MemoryFuseImpl::allocate_fh_int()
{
  {
    auto locked = this->state_.lock();

    if (!locked->available_fhs_.empty()) {
      const FuseFileHandle fh = locked->available_fhs_.back();
      locked->available_fhs_.pop_back();
      return fh;
    }
  }
  return FuseFileHandle{this->next_unused_fh_int_.fetch_add(1)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<batt::SharedPtr<MemInode>> MemoryFuseImpl::find_inode(fuse_ino_t ino)
{
  auto locked = this->state_.lock();

  auto iter = locked->inodes_.find(ino);
  if (iter == locked->inodes_.end()) {
    LLFS_VLOG(1) << "Bad ino: " << ino;
    return {batt::status_from_errno(ENOENT)};
  }
  return {iter->second};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<batt::SharedPtr<MemFileHandle>> MemoryFuseImpl::find_file_handle(FuseFileHandle fh)
{
  auto locked = this->state_.lock();

  auto iter = locked->file_handles_.find(fh);
  if (iter == locked->file_handles_.end()) {
    LLFS_VLOG(1) << "Bad fh: " << fh << BATT_INSPECT_RANGE(locked->file_handles_);
    return {batt::status_from_errno(EINVAL)};
  }
  return {iter->second};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<batt::SharedPtr<MemInode>> MemoryFuseImpl::create_inode_impl(fuse_ino_t parent,
                                                                            const std::string& name,
                                                                            mode_t mode,
                                                                            MemInode::IsDir is_dir)
{
  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> parent_inode, this->find_inode(parent));

  const auto category = [&] {
    if (is_dir) {
      return MemInode::Category::kDirectory;
    }
    return static_cast<MemInode::Category>(mode & S_IFMT);
  }();

  const fuse_ino_t new_ino = this->allocate_ino_int();
  auto new_inode = batt::make_shared<MemInode>(new_ino,   //
                                               category,  //
                                               (mode & (S_IRWXU | S_IRWXG | S_IRWXO)));

  BATT_REQUIRE_OK(parent_inode->add_child(name, batt::make_copy(new_inode)));

  {
    auto locked = this->state_.lock();
    locked->inodes_.emplace(new_ino, new_inode);
  }

  new_inode->add_lookup(1);

  return {std::move(new_inode)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status MemoryFuseImpl::close_impl(FuseFileHandle fh)
{
  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemFileHandle> file_handle, this->find_file_handle(fh));

  {
    auto locked = this->state_.lock();

    locked->file_handles_.erase(fh);
    locked->available_fhs_.emplace_back(fh);

    LLFS_VLOG(1) << "close_impl(" << fh << ")" << BATT_INSPECT_RANGE(locked->available_fhs_);
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto MemoryFuseImpl::readdir_impl(fuse_req_t req, fuse_ino_t ino, size_t size, DirentOffset offset,
                                  FuseFileHandle fh, PlusApi plus_api)
    -> batt::StatusOr<FuseReadDirData>
{
  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));
  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemFileHandle> dh, this->find_file_handle(fh));

  return inode->readdir(req, *dh, size, offset, plus_api);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status MemoryFuseImpl::unlink_impl(fuse_req_t req, fuse_ino_t parent, const std::string& name,
                                         MemInode::IsDir is_dir)
{
  using ResultPair = std::pair<MemInode::IsDead, batt::SharedPtr<MemInode>>;

  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> parent_inode, this->find_inode(parent));
  BATT_ASSIGN_OK_RESULT(ResultPair child_inode,
                        parent_inode->remove_child(name, is_dir, MemInode::RequireEmpty{is_dir}));

  if (child_inode.first) {
    auto locked = this->state_.lock();

    locked->inodes_.erase(child_inode.second->get_ino());
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<usize> MemoryFuseImpl::write_impl(
    fuse_req_t req, fuse_ino_t ino, const batt::Slice<const batt::ConstBuffer>& buffers,
    FileOffset offset, FuseFileHandle fh)
{
  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));
  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemFileHandle> dh, this->find_file_handle(fh));

  return {inode->write(offset, buffers)};
}

}  //namespace llfs
