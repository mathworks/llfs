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

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/suppress.hpp>

#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

namespace llfs {

BATT_SUPPRESS_IF_GCC("-Wunused-parameter")

/** \brief A minimal example of a FuseImpl class.
 */
class MemoryFuseImpl : public WorkerTaskFuseImpl<MemoryFuseImpl>
{
 public:
  struct MemFileHandle;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class MemInode : public batt::RefCounted<MemInode>
  {
   public:
    BATT_STRONG_TYPEDEF(bool, IsDead);

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
    static const u64 kUnlinkedFlag = u64{1} << 60;
    static const u64 kLookupCountMask = kUnlinkedFlag - 1;

    //----- --- -- -  -  -   -

    explicit MemInode(fuse_ino_t ino, Category category) noexcept;

    //----- --- -- -  -  -   -

    const fuse_entry_param* get_fuse_entry_param() const noexcept
    {
      auto locked = this->state_.lock();
      return &locked->entry_;
    }

    batt::Status add_child_entry(fuse_req_t req, const std::string& name,
                                 batt::SharedPtr<MemInode>&& child_inode);

    //----- --- -- -  -  -   -

    batt::StatusOr<const fuse_entry_param*> lookup(const std::string& name);

    IsDead forget(u64 count);

    batt::StatusOr<FuseImplBase::Attributes> get_attributes(fuse_req_t req, fuse_file_info* fi);

    batt::StatusOr<FuseImplBase::Attributes> set_attributes(fuse_req_t req, fuse_file_info* fi,
                                                            struct stat* attr, int to_set);

    batt::StatusOr<FuseReadDirData> readdir(MemFileHandle& dh, fuse_req_t req, size_t size,
                                            DirentOffset offset, fuse_file_info* fi,
                                            PlusApi plus_api);

    //----- --- -- -  -  -   -
   private:
    void add_lookup(usize count = 1) noexcept;

    IsDead remove_lookup(usize count = 1) noexcept;

    void init_directory();

    //----- --- -- -  -  -   -
    struct State {
      fuse_entry_param entry_;
      std::unordered_map<std::string, batt::SharedPtr<MemInode>> children_by_name_;
      std::vector<std::pair<batt::SharedPtr<MemInode>, std::string>> children_by_offset_;

      State(fuse_ino_t ino, Category category) noexcept;

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct OpenDirState {
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct OpenFileState {
    FileOffset offset;
  };

  using OpenState = std::variant<OpenFileState, OpenDirState>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct MemFileHandle : batt::RefCounted<MemFileHandle> {
    batt::SharedPtr<MemInode> inode;
    fuse_file_info& info;
    batt::Mutex<OpenState> state;

    template <typename... StateInitArgs>
    explicit MemFileHandle(batt::SharedPtr<MemInode>&& inode, fuse_file_info& info,
                           StateInitArgs&&... state_init_args) noexcept
        : inode{std::move(inode)}
        , info{info}
        , state{BATT_FORWARD(state_init_args)...}
    {
      BATT_CHECK_NOT_NULLPTR(&info);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<fuse_ino_t> next_unused_ino_{FUSE_ROOT_ID + 1};
  std::atomic<u64> next_unused_fh_int_{0};

  struct State {
    std::unordered_map<fuse_ino_t, batt::SharedPtr<MemInode>> inodes_;

    std::unordered_map<u64, batt::SharedPtr<MemFileHandle>> file_handles_;

    std::vector<u64> available_fhs_;
  };

  batt::Mutex<State> state_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemoryFuseImpl(std::shared_ptr<WorkQueue>&& work_queue) noexcept
      : WorkerTaskFuseImpl<MemoryFuseImpl>{std::move(work_queue)}
  {
    auto locked = this->state_.lock();

    locked->inodes_.emplace(
        FUSE_ROOT_ID, batt::make_shared<MemInode>(FUSE_ROOT_ID, MemInode::Category::kDirectory));
  }

  /** \brief Returns an unused inode (ino) integer.
   */
  fuse_ino_t allocate_ino_int()
  {
    return this->next_unused_ino_.fetch_add(1);
  }

  /** \brief Returns an unused file handle integer.
   */
  u64 allocate_fh_int()
  {
    {
      auto locked = this->state_.lock();

      if (!locked->available_fhs_.empty()) {
        const u64 fh = locked->available_fhs_.back();
        locked->available_fhs_.pop_back();
        return fh;
      }
    }
    return this->next_unused_fh_int_.fetch_add(1);
  }

  /** \brief
   */
  batt::StatusOr<batt::SharedPtr<MemInode>> find_inode(fuse_ino_t ino)
  {
    auto locked = this->state_.lock();

    auto iter = locked->inodes_.find(ino);
    if (iter == locked->inodes_.end()) {
      return {batt::status_from_errno(ENOENT)};
    }
    return {iter->second};
  }

  /** \brief
   */
  batt::StatusOr<batt::SharedPtr<MemFileHandle>> find_file_handle(fuse_file_info* fi)
  {
    auto locked = this->state_.lock();

    BATT_CHECK_NOT_NULLPTR(fi);

    auto iter = locked->file_handles_.find(fi->fh);
    if (iter == locked->file_handles_.end()) {
      return {batt::status_from_errno(EINVAL)};
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
                                                 const std::string& name)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(parent)         //
                 << "," << BATT_INSPECT_STR(name)       //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> parent_inode,  //
                          this->find_inode(parent));

    return parent_inode->lookup(name);
  }

  /** \brief
   */ // 4/44
  void forget_inode(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(nlookup)        //
                 << " )";

    batt::StatusOr<batt::SharedPtr<MemInode>> inode = this->find_inode(ino);
    if (!inode.ok()) {
      LLFS_LOG_ERROR() << "Inode not found!  " << BATT_INSPECT(ino) << BATT_INSPECT(inode.status());
      return;
    }

    // TODO [tastolfi 2023-07-11] Do something with the result here.
    //
    [[maybe_unused]] const MemInode::IsDead is_dead = (*inode)->forget(nlookup);
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

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return inode->get_attributes(req, fi);
  }

  /** \brief
   */ // 6/44
  batt::StatusOr<FuseImplBase::Attributes> set_attributes(fuse_req_t req, fuse_ino_t ino,
                                                          struct stat* attr, int to_set,
                                                          fuse_file_info* fi)
  {
    /*
     * If the setattr was invoked from the ftruncate() system call
     * under Linux kernel versions 2.6.15 or later, the fi->fh will
     * contain the value set by the open method or will be undefined
     * if the open method didn't set any value.  Otherwise (not
     * ftruncate call, or kernel version earlier than 2.6.15) the fi
     * parameter will be NULL.
     */

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return inode->set_attributes(req, fi, attr, to_set);
  }

  /** \brief
   */ // 7/44
  const char* readlink(fuse_req_t req, fuse_ino_t ino)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return "";
  }

  /** \brief
   */ // 8/44
  batt::StatusOr<const fuse_entry_param*> make_node(fuse_req_t req, fuse_ino_t parent,
                                                    const std::string& name, mode_t mode,
                                                    dev_t rdev)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 9/44
  batt::StatusOr<const fuse_entry_param*> make_directory(fuse_req_t req, fuse_ino_t parent,
                                                         const std::string& name, mode_t mode)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 10/44
  batt::Status unlink(fuse_req_t req, fuse_ino_t parent, const std::string& name)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << BATT_INSPECT(req)                   //
                 << BATT_INSPECT(parent)                //
                 << BATT_INSPECT_STR(name);

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 11/44
  batt::Status remove_directory(fuse_req_t req, fuse_ino_t parent, const std::string& name)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 12/44
  batt::StatusOr<const fuse_entry_param*> symbolic_link(fuse_req_t req, const std::string& link,
                                                        fuse_ino_t parent, const std::string& name)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 13/44
  batt::Status rename(fuse_req_t req, fuse_ino_t parent, const std::string& name,
                      fuse_ino_t newparent, const std::string& newname, unsigned int flags)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 14/44
  batt::StatusOr<const fuse_entry_param*> hard_link(fuse_req_t req, fuse_ino_t ino,
                                                    fuse_ino_t newparent,
                                                    const std::string& newname)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 15/44
  batt::StatusOr<const fuse_file_info*> open(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << BATT_INSPECT(req)                   //
                 << BATT_INSPECT(ino)                   //
                 << BATT_INSPECT(fi);

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return this->open_inode_impl(std::move(inode), fi);
  }

  /** \brief
   */ // 16/44
  batt::StatusOr<FuseImplBase::FuseReadData> read(fuse_req_t req, fuse_ino_t ino, size_t size,
                                                  FileOffset offset, fuse_file_info* fi)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 17/44
  batt::StatusOr<usize> write(fuse_req_t req, fuse_ino_t ino, const batt::ConstBuffer& buffer,
                              FileOffset offset, fuse_file_info* fi)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 18/44
  batt::Status flush(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << BATT_INSPECT(req)                   //
                 << BATT_INSPECT(ino)                   //
                 << BATT_INSPECT(fi);

    return batt::OkStatus();
  }

  /** \brief
   */ // 19/44
  batt::Status release(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << BATT_INSPECT(req)                   //
                 << BATT_INSPECT(ino)                   //
                 << BATT_INSPECT(fi);

    return this->close_impl(fi);
  }

  /** \brief
   */ // 20/44
  batt::Status fsync(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 21/44
  batt::StatusOr<const fuse_file_info*> opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    BATT_CHECK_NOT_NULLPTR(fi);

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return this->open_inode_impl(std::move(inode), fi, OpenDirState{});
  }

  /** \brief
   */ // 22/44
  batt::StatusOr<FuseImplBase::FuseReadDirData> readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                                                        DirentOffset offset, fuse_file_info* fi)
  {
    return this->readdir_impl(req, ino, size, offset, fi, PlusApi{false});
  }

  /** \brief
   */ // 23/44
  batt::Status releasedir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << BATT_INSPECT(req)                   //
                 << BATT_INSPECT(ino)                   //
                 << BATT_INSPECT(fi);

    return this->close_impl(fi);
  }

  /** \brief
   */ // 24/44
  batt::Status fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 25/44
  batt::StatusOr<const struct statvfs*> statfs(fuse_req_t req, fuse_ino_t ino)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 26/44
  batt::Status set_extended_attribute(fuse_req_t req, fuse_ino_t ino,
                                      const FuseImplBase::ExtendedAttribute& attr, int flags)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 27/44
  batt::StatusOr<FuseImplBase::FuseGetExtendedAttributeReply> get_extended_attribute(
      fuse_req_t req, fuse_ino_t ino, const std::string& name, size_t size)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 29/44
  batt::Status remove_extended_attribute(fuse_req_t req, fuse_ino_t ino, const std::string& name)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 30/44
  batt::Status check_access(fuse_req_t req, fuse_ino_t ino, int mask)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(mask)           //
                 << " )";

    return batt::OkStatus();
  }

  /** \brief
   */ // 31/44
  batt::StatusOr<FuseImplBase::FuseCreateReply> create(fuse_req_t req, fuse_ino_t parent,
                                                       const std::string& name, mode_t mode,
                                                       fuse_file_info* fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__       //
                 << "(" << BATT_INSPECT(req)                 //
                 << "," << BATT_INSPECT(parent)              //
                 << "," << BATT_INSPECT_STR(name)            //
                 << "," << BATT_INSPECT(DumpFileMode{mode})  //
                 << "," << BATT_INSPECT(fi)                  //
                 << " )";

    BATT_CHECK_NOT_NULLPTR(fi);

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> parent_inode, this->find_inode(parent));

    const auto category = (MemInode::Category)(mode & S_IFMT);
    const fuse_ino_t new_ino = this->allocate_ino_int();
    auto new_inode = batt::make_shared<MemInode>(new_ino, category);

    BATT_REQUIRE_OK(parent_inode->add_child_entry(req, name, batt::make_copy(new_inode)));

    {
      auto locked = this->state_.lock();
      locked->inodes_.emplace(new_ino, new_inode);
    }

    BATT_ASSIGN_OK_RESULT(const fuse_file_info* opened_file_info,
                          this->open_inode_impl(batt::make_copy(new_inode), fi));

    return FuseImplBase::FuseCreateReply{
        .entry = new_inode->get_fuse_entry_param(),
        .fi = opened_file_info,
    };
  }

  /** \brief
   */ // 38/44
  void retrieve_reply(fuse_req_t req, void* cookie, fuse_ino_t ino, off_t offset,
                      struct fuse_bufvec* bufv)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";
  }

  /** \brief
   */ // 39/44
  void forget_multiple_inodes(fuse_req_t req, batt::Slice<fuse_forget_data> forgets)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";
  }

  /** \brief
   */ // 41/44
  batt::Status file_allocate(fuse_req_t req, fuse_ino_t ino, int mode, FileOffset offset,
                             FileLength length, fuse_file_info* fi)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 42/44
  batt::StatusOr<FuseImplBase::FuseReadDirData> readdirplus(fuse_req_t req, fuse_ino_t ino,
                                                            size_t size, DirentOffset offset,
                                                            fuse_file_info* fi)
  {
    return this->readdir_impl(req, ino, size, offset, fi, PlusApi{true});
  }

 private:
  template <typename... OpenStateArgs>
  batt::StatusOr<const fuse_file_info*> open_inode_impl(batt::SharedPtr<MemInode>&& inode,
                                                        fuse_file_info* fi,
                                                        OpenStateArgs&&... open_state_args)
  {
    BATT_CHECK_NOT_NULLPTR(inode);
    BATT_CHECK_NOT_NULLPTR(fi);

    fi->fh = this->allocate_fh_int();

    {
      auto locked = this->state_.lock();

      auto [fh_iter, inserted] = locked->file_handles_.emplace(
          fi->fh, batt::make_shared<MemFileHandle>(std::move(inode), *fi,
                                                   BATT_FORWARD(open_state_args)...));

      BATT_CHECK(inserted);

      return &fh_iter->second->info;
    }
  }

  batt::Status close_impl(fuse_file_info* fi)
  {
    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemFileHandle> file_handle, this->find_file_handle(fi));

    BATT_CHECK_NOT_NULLPTR(fi);
    const u64 fh = fi->fh;
    {
      auto locked = this->state_.lock();

      locked->file_handles_.erase(fh);
      locked->available_fhs_.emplace_back(fh);
    }

    return batt::OkStatus();
  }

  batt::StatusOr<FuseReadDirData> readdir_impl(fuse_req_t req, fuse_ino_t ino, size_t size,
                                               DirentOffset offset, fuse_file_info* fi,
                                               PlusApi plus_api)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(size)           //
                 << "," << BATT_INSPECT(offset)         //
                 << "," << BATT_INSPECT(fi)             //
                 << "," << BATT_INSPECT(plus_api)       //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));
    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemFileHandle> dh, this->find_file_handle(fi));

    return inode->readdir(*dh, req, size, offset, fi, plus_api);
  }

};  // class MemoryFuseImpl

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  //namespace llfs

#endif  // LLFS_MEM_FUSE_HPP
