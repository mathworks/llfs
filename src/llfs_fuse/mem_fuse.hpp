//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEM_FUSE_HPP
#define LLFS_MEM_FUSE_HPP

#include <llfs/fuse.hpp>

#include <cstring>
#include <string>
#include <unordered_map>

namespace llfs {

/** \brief A minimal example of a FuseImpl class.
 */
class MemoryFuseImpl : public WorkerTaskFuseImpl<MemoryFuseImpl>
{
 public:
  enum struct InodeKind : mode_t {
    kBlockSpecial = S_IFBLK,
    kCharSpecial = S_IFCHR,
    kFifoSpecial = S_IFIFO,
    kRegularFile = S_IFREG,
    kDirectory = S_IFDIR,
    kSymbolicLink = S_IFLNK,
  };

  struct MemInode {
    using Children = std::unordered_map<std::string, fuse_ino_t>;

    fuse_entry_param entry;
    Children children;

    explicit MemInode(fuse_ino_t ino, InodeKind ino_kind) noexcept
    {
      std::memset(&this->entry, 0, sizeof(this->entry));
      this->entry.ino = ino;
      this->entry.attr.st_ino = ino;
      this->entry.attr.st_mode = (mode_t)ino_kind | 0755;
      this->entry.attr.st_uid = 1001;
      this->entry.attr.st_gid = 1001;
      this->entry.attr.st_blksize = 4096;
      this->entry.attr.st_blocks = 8;
    }

    batt::StatusOr<fuse_ino_t> find_child(const char* child_name)
    {
      auto child_iter = this->children.find(child_name);
      if (child_iter == this->children.end()) {
        return {batt::status_from_errno(ENOENT)};
      }
      return child_iter->second;
    }

    batt::StatusOr<std::shared_ptr<MemInode>> find_child_inode(const char* child_name,
                                                               MemoryFuseImpl* impl)
    {
      BATT_ASSIGN_OK_RESULT(fuse_ino_t ino, this->find_child(child_name));
      return impl->find_inode(ino);
    }
  };

  struct OpenDirState {
  };

  struct OpenFileState {
  };

  using OpenState = std::variant<OpenFileState, OpenDirState>;

  struct MemFileHandle {
    std::shared_ptr<MemInode> inode;
    fuse_file_info& info;
    OpenState state;

    template <typename... StateInitArgs>
    explicit MemFileHandle(std::shared_ptr<MemInode>&& inode, fuse_file_info& info,
                           StateInitArgs&&... state_init_args) noexcept
        : inode{std::move(inode)}
        , info{info}
        , state{BATT_FORWARD(state_init_args)...}
    {
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::vector<u64> available_fhs_;

  std::atomic<u64> next_unused_fh_int_{0};

  std::unordered_map<fuse_ino_t, std::shared_ptr<MemInode>> inodes_;

  std::unordered_map<u64, std::shared_ptr<MemFileHandle>> file_handles_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemoryFuseImpl(std::shared_ptr<WorkQueue>&& work_queue) noexcept
      : WorkerTaskFuseImpl<MemoryFuseImpl>{std::move(work_queue)}
  {
    this->inodes_.emplace(FUSE_ROOT_ID,
                          std::make_shared<MemInode>(FUSE_ROOT_ID, InodeKind::kDirectory));
  }

  /** \brief Returns an unused file handle integer.
   */
  u64 allocate_fh_int()
  {
    if (!this->available_fhs_.empty()) {
      const u64 fh = this->available_fhs_.back();
      this->available_fhs_.pop_back();
      return fh;
    }
    return this->next_unused_fh_int_.fetch_add(1);
  }

  /** \brief
   */
  batt::StatusOr<std::shared_ptr<MemInode>> find_inode(fuse_ino_t ino)
  {
    auto iter = this->inodes_.find(ino);
    if (iter == this->inodes_.end()) {
      return {batt::status_from_errno(ENOENT)};
    }
    return {iter->second};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief
   */
  void init()
  {
    BATT_CHECK_NOT_NULLPTR(this->conn_);
  }

  /** \brief
   */
  void destroy()
  {
    BATT_CHECK_NOT_NULLPTR(this->conn_);
  }

  /** \brief
   */
  batt::StatusOr<const fuse_entry_param*> lookup(fuse_req_t req, fuse_ino_t parent,
                                                 const char* name)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(parent)         //
                 << "," << BATT_INSPECT(name)           //
                 << " )";

    BATT_ASSIGN_OK_RESULT(std::shared_ptr<MemInode> parent_inode,  //
                          this->find_inode(parent));

    BATT_ASSIGN_OK_RESULT(std::shared_ptr<MemInode> child_inode,  //
                          parent_inode->find_child_inode(name, this));

    return {&child_inode->entry};
  }

  /** \brief
   */
  template <typename Handler>
  void async_forget_inode(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)nlookup;

    BATT_FORWARD(handler)();
  }

  /** \brief
   */
  batt::StatusOr<FuseImplBase::Attributes> get_attributes(fuse_req_t req, fuse_ino_t ino,
                                                          fuse_file_info* fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(fi)             //
                 << " )";

    BATT_ASSIGN_OK_RESULT(std::shared_ptr<MemInode> inode, this->find_inode(ino));

    return {FuseImplBase::Attributes{
        .attr = &inode->entry.attr,
        .timeout_sec = 0.0,
    }};
  }

  /** \brief
   */
  template <typename Handler>
  void async_set_attributes(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
                            fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)attr;
    (void)to_set;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseImplBase::Attributes>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_readlink(fuse_req_t req, fuse_ino_t ino, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;

    BATT_FORWARD(handler)(/*link=*/"");
  }

  /** \brief
   */
  template <typename Handler>
  void async_make_node(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode, dev_t rdev,
                       Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)parent;
    (void)name;
    (void)mode;
    (void)rdev;

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_entry_param*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_make_directory(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
                            Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)parent;
    (void)name;
    (void)mode;

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_entry_param*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_unlink(fuse_req_t req, fuse_ino_t parent, const char* name, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)parent;
    (void)name;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_remove_directory(fuse_req_t req, fuse_ino_t parent, const char* name,
                              Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)parent;
    (void)name;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_symbolic_link(fuse_req_t req, const char* link, fuse_ino_t parent, const char* name,
                           Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)link;
    (void)parent;
    (void)name;

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_entry_param*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_rename(fuse_req_t req, fuse_ino_t parent, const char* name, fuse_ino_t newparent,
                    const char* newname, unsigned int flags, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)parent;
    (void)name;
    (void)newparent;
    (void)newname;
    (void)flags;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_hard_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char* newname,
                       Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)newparent;
    (void)newname;

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_entry_param*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_open(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_file_info*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_read(fuse_req_t req, fuse_ino_t ino, size_t size, FileOffset off, fuse_file_info* fi,
                  Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)size;
    (void)off;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseReadData>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_write(fuse_req_t req, fuse_ino_t ino, const batt::ConstBuffer& buffer,
                   FileOffset offset, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)buffer;
    (void)offset;
    (void)fi;

    BATT_FORWARD(handler)(batt::StatusOr<usize>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_flush(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fi;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_release(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fi;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi,
                   Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)datasync;
    (void)fi;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fi);

    BATT_CHECK_NOT_NULLPTR(fi);

    auto inode_iter = this->inodes_.find(ino);
    if (inode_iter == this->inodes_.end()) {
      BATT_FORWARD(handler)({batt::status_from_errno(ENOENT)});
      return;
    }

    fi->fh = this->allocate_fh_int();

    auto [fh_iter, inserted] = this->file_handles_.emplace(
        fi->fh, std::make_shared<MemFileHandle>(batt::make_copy(inode_iter->second), *fi));

    BATT_CHECK(inserted);

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_file_info*>{&fh_iter->second->info});
  }

  /** \brief
   */
  template <typename Handler>
  void async_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, FileOffset off,
                     fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)size;
    (void)off;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseReadDirData>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_releasedir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fi;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi,
                      Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)datasync;
    (void)fi;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_statfs(fuse_req_t req, fuse_ino_t ino, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;

    BATT_FORWARD(handler)
    (batt::StatusOr<const struct statvfs*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_set_extended_attribute(fuse_req_t req, fuse_ino_t ino,
                                    const FuseImplBase::ExtendedAttribute& attr, int flags,
                                    Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)attr;
    (void)flags;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_get_extended_attribute(fuse_req_t req, fuse_ino_t ino, const char* name, size_t size,
                                    Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)name;
    (void)size;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseGetExtendedAttributeReply>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
  template <typename Handler>
  void async_remove_extended_attribute(fuse_req_t req, fuse_ino_t ino, const char* name,
                                       Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)name;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_retrieve_reply(fuse_req_t req, void* cookie, fuse_ino_t ino, off_t offset,
                            struct fuse_bufvec* bufv, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)cookie;
    (void)ino;
    (void)offset;
    (void)bufv;

    BATT_FORWARD(handler)();
  }

  /** \brief
   */
  template <typename Handler>
  void async_forget_multiple_inodes(fuse_req_t req, batt::Slice<fuse_forget_data> forgets,
                                    Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)forgets;

    BATT_FORWARD(handler)();
  }

  /** \brief
   */
  template <typename Handler>
  void async_check_access(fuse_req_t req, fuse_ino_t ino, int mask, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(mask);

    BATT_FORWARD(handler)(batt::OkStatus());
  }

  /** \brief
   */
  template <typename Handler>
  void async_file_allocate(fuse_req_t req, fuse_ino_t ino, int mode, FileOffset offset,
                           FileLength length, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)mode;
    (void)offset;
    (void)length;
    (void)fi;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */
  template <typename Handler>
  void async_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, FileOffset offset,
                         fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(size) << BATT_INSPECT(offset) << BATT_INSPECT(fi);

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseReadDirData>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

 private:
  template <typename Handler>
  void async_readdir_impl(fuse_req_t req, fuse_ino_t ino, size_t size, FileOffset offset,
                          fuse_file_info* fi, const bool plus, Handler&& handler)
  {
    auto iter = this->inodes_.find(ino);
  }

};  // class MemoryFuseImpl

}  //namespace llfs

#endif  // LLFS_MEM_FUSE_HPP
