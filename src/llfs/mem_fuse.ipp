//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEM_FUSE_IPP
#define LLFS_MEM_FUSE_IPP

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... OpenStateArgs>
inline batt::StatusOr<const fuse_file_info*> MemoryFuseImpl::open_inode_impl(
    batt::SharedPtr<MemInode>&& inode, const fuse_file_info& fi, OpenStateArgs&&... open_state_args)
{
  BATT_CHECK_NOT_NULLPTR(inode);

  const u64 fh = this->allocate_fh_int();
  LLFS_VLOG(1) << "open_inode_impl, " << BATT_INSPECT(fh);

  {
    auto locked = this->state_.lock();

    auto [fh_iter, inserted] = locked->file_handles_.emplace(
        fh, batt::make_shared<MemFileHandle>(fh, std::move(inode), fi,
                                             BATT_FORWARD(open_state_args)...));

    BATT_CHECK(inserted);

    return &fh_iter->second->info;
  }
}

}  //namespace llfs

#endif  // LLFS_MEM_FUSE_IPP
