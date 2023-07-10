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
    enum struct Category : mode_t {
      kBlockSpecial = S_IFBLK,
      kCharSpecial = S_IFCHR,
      kFifoSpecial = S_IFIFO,
      kRegularFile = S_IFREG,
      kDirectory = S_IFDIR,
      kSymbolicLink = S_IFLNK,
    };

    //----- --- -- -  -  -   -

    explicit MemInode(fuse_ino_t ino, Category category) noexcept;

    //----- --- -- -  -  -   -

    const fuse_entry_param* get_fuse_entry_param() const noexcept
    {
      return &this->entry_;
    }

    batt::Status add_child_entry(fuse_req_t req, const char* name,
                                 batt::SharedPtr<MemInode>&& child_inode);

    //----- --- -- -  -  -   -

    batt::StatusOr<const fuse_entry_param*> lookup(const char* name);

    batt::StatusOr<FuseImplBase::Attributes> get_attributes(fuse_req_t req, fuse_file_info* fi);

    batt::Status set_attributes(fuse_req_t req, fuse_file_info* fi, struct stat* attr, int to_set);

    batt::StatusOr<FuseReadDirData> readdir(MemFileHandle& dh, fuse_req_t req, size_t size,
                                            DirentOffset offset, fuse_file_info* fi,
                                            PlusApi plus_api);

    //----- --- -- -  -  -   -
   private:
    void init_directory();

    batt::StatusOr<batt::SharedPtr<MemInode>> find_child_by_name(
        const char* child_name, std::unique_lock<std::mutex>& lock);

    batt::Status pack_as_fuse_dir_entry(fuse_req_t req, batt::ConstBuffer& out_buf,
                                        batt::MutableBuffer& dst_buf, const char* name,
                                        DirentOffset offset, PlusApi plus_api,
                                        std::unique_lock<std::mutex>& lock) const;

    //----- --- -- -  -  -   -
    std::mutex mutex_;
    fuse_entry_param entry_;
    std::unordered_map<std::string, batt::SharedPtr<MemInode>> children_by_name_;
    std::vector<std::pair<batt::SharedPtr<MemInode>, std::string>> children_by_offset_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct OpenDirState {
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct OpenFileState {
  };

  using OpenState = std::variant<OpenFileState, OpenDirState>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct MemFileHandle : batt::RefCounted<MemFileHandle> {
    batt::SharedPtr<MemInode> inode;
    fuse_file_info& info;
    OpenState state;

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

  std::mutex mutex_;

  std::unordered_map<fuse_ino_t, batt::SharedPtr<MemInode>> inodes_;

  std::unordered_map<u64, batt::SharedPtr<MemFileHandle>> file_handles_;

  std::vector<u64> available_fhs_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemoryFuseImpl(std::shared_ptr<WorkQueue>&& work_queue) noexcept
      : WorkerTaskFuseImpl<MemoryFuseImpl>{std::move(work_queue)}
  {
    this->inodes_.emplace(
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
      std::unique_lock<std::mutex> lock{this->mutex_};

      if (!this->available_fhs_.empty()) {
        const u64 fh = this->available_fhs_.back();
        this->available_fhs_.pop_back();
        return fh;
      }
    }
    return this->next_unused_fh_int_.fetch_add(1);
  }

  /** \brief
   */
  batt::StatusOr<batt::SharedPtr<MemInode>> find_inode(fuse_ino_t ino)
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    auto iter = this->inodes_.find(ino);
    if (iter == this->inodes_.end()) {
      return {batt::status_from_errno(ENOENT)};
    }
    return {iter->second};
  }

  /** \brief
   */
  batt::StatusOr<batt::SharedPtr<MemFileHandle>> find_file_handle(fuse_file_info* fi)
  {
    std::unique_lock<std::mutex> lock{this->mutex_};

    BATT_CHECK_NOT_NULLPTR(fi);

    auto iter = this->file_handles_.find(fi->fh);
    if (iter == this->file_handles_.end()) {
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
                                                 const char* name)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(parent)         //
                 << "," << BATT_INSPECT(name)           //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> parent_inode,  //
                          this->find_inode(parent));

    return parent_inode->lookup(name);
  }

  /** \brief
   */ // 4/44
  void forget_inode(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
  {
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
    return "";
  }

  /** \brief
   */ // 8/44
  batt::StatusOr<const fuse_entry_param*> make_node(fuse_req_t req, fuse_ino_t parent,
                                                    const char* name, mode_t mode, dev_t rdev)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 9/44
  batt::StatusOr<const fuse_entry_param*> make_directory(fuse_req_t req, fuse_ino_t parent,
                                                         const char* name, mode_t mode)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 10/44
  batt::Status unlink(fuse_req_t req, fuse_ino_t parent, const char* name)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 11/44
  batt::Status remove_directory(fuse_req_t req, fuse_ino_t parent, const char* name)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 12/44
  batt::StatusOr<const fuse_entry_param*> symbolic_link(fuse_req_t req, const char* link,
                                                        fuse_ino_t parent, const char* name)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 13/44
  batt::Status rename(fuse_req_t req, fuse_ino_t parent, const char* name, fuse_ino_t newparent,
                      const char* newname, unsigned int flags)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 14/44
  batt::StatusOr<const fuse_entry_param*> hard_link(fuse_req_t req, fuse_ino_t ino,
                                                    fuse_ino_t newparent, const char* newname)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 15/44
  batt::StatusOr<const fuse_file_info*> open(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 16/44
  batt::StatusOr<FuseImplBase::FuseReadData> read(fuse_req_t req, fuse_ino_t ino, size_t size,
                                                  FileOffset offset, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 17/44
  batt::StatusOr<usize> write(fuse_req_t req, fuse_ino_t ino, const batt::ConstBuffer& buffer,
                              FileOffset offset, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 18/44
  batt::Status flush(fuse_req_t /*req*/, fuse_ino_t /*ino*/, fuse_file_info* /*fi*/)
  {
    return batt::OkStatus();
  }

  /** \brief
   */ // 19/44
  batt::Status release(fuse_req_t /*req*/, fuse_ino_t /*ino*/, fuse_file_info* fi)
  {
    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemFileHandle> file_handle, this->find_file_handle(fi));

    BATT_CHECK_NOT_NULLPTR(fi);
    const u64 fh = fi->fh;

    {
      std::unique_lock<std::mutex> lock{this->mutex_};

      this->file_handles_.erase(fh);
      this->available_fhs_.emplace_back(fh);
    }

    return batt::OkStatus();
  }

  /** \brief
   */ // 20/44
  batt::Status fsync(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 21/44
  batt::StatusOr<const fuse_file_info*> opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    BATT_CHECK_NOT_NULLPTR(fi);

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return this->open_inode_impl(std::move(inode), fi);
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
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 24/44
  batt::Status fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 25/44
  batt::StatusOr<const struct statvfs*> statfs(fuse_req_t req, fuse_ino_t ino)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 26/44
  batt::Status set_extended_attribute(fuse_req_t req, fuse_ino_t ino,
                                      const FuseImplBase::ExtendedAttribute& attr, int flags)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 27/44
  batt::StatusOr<FuseImplBase::FuseGetExtendedAttributeReply> get_extended_attribute(
      fuse_req_t req, fuse_ino_t ino, const char* name, size_t size)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 29/44
  batt::Status remove_extended_attribute(fuse_req_t req, fuse_ino_t ino, const char* name)
  {
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
                                                       const char* name, mode_t mode,
                                                       fuse_file_info* fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__       //
                 << "(" << BATT_INSPECT(req)                 //
                 << "," << BATT_INSPECT(parent)              //
                 << "," << BATT_INSPECT(name)                //
                 << "," << BATT_INSPECT(DumpFileMode{mode})  //
                 << "," << BATT_INSPECT(fi)                  //
                 << " )";

    BATT_CHECK_NOT_NULLPTR(fi);

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> parent_inode, this->find_inode(parent));

    const auto category = (MemInode::Category)(mode & S_IFMT);
    const fuse_ino_t new_ino = this->allocate_ino_int();
    auto new_inode = batt::make_shared<MemInode>(new_ino, category);

    this->inodes_.emplace(new_ino, new_inode);
    BATT_REQUIRE_OK(parent_inode->add_child_entry(req, name, batt::make_copy(new_inode)));

    BATT_ASSIGN_OK_RESULT(const fuse_file_info* open_fi,
                          this->open_inode_impl(batt::make_copy(new_inode), fi));

    return FuseImplBase::FuseCreateReply{
        .entry = new_inode->get_fuse_entry_param(),
        .fi = open_fi,
    };
  }

  /** \brief
   */ // 38/44
  void retrieve_reply(fuse_req_t req, void* cookie, fuse_ino_t ino, off_t offset,
                      struct fuse_bufvec* bufv)
  {
  }

  /** \brief
   */ // 39/44
  void forget_multiple_inodes(fuse_req_t req, batt::Slice<fuse_forget_data> forgets)
  {
  }

  /** \brief
   */ // 41/44
  batt::Status file_allocate(fuse_req_t req, fuse_ino_t ino, int mode, FileOffset offset,
                             FileLength length, fuse_file_info* fi)
  {
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
  batt::StatusOr<const fuse_file_info*> open_inode_impl(batt::SharedPtr<MemInode>&& inode,
                                                        fuse_file_info* fi)
  {
    BATT_CHECK_NOT_NULLPTR(inode);
    BATT_CHECK_NOT_NULLPTR(fi);

    fi->fh = this->allocate_fh_int();

    auto [fh_iter, inserted] = this->file_handles_.emplace(
        fi->fh, batt::make_shared<MemFileHandle>(std::move(inode), *fi));

    BATT_CHECK(inserted);

    return &fh_iter->second->info;
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline /*explicit*/ MemoryFuseImpl::MemInode::MemInode(fuse_ino_t ino, Category category) noexcept
    : entry_{}
    , children_by_name_{}
    , children_by_offset_{}
{
  std::memset(&this->entry_, 0, sizeof(this->entry_));
  this->entry_.ino = ino;
  this->entry_.attr.st_ino = ino;
  this->entry_.attr.st_mode = (mode_t)category | 0755;
  this->entry_.attr.st_uid = 1001;
  this->entry_.attr.st_gid = 1001;
  this->entry_.attr.st_blksize = 4096;
  this->entry_.attr.st_blocks = 8;

  if (category == Category::kDirectory) {
    this->init_directory();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void MemoryFuseImpl::MemInode::init_directory()
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto MemoryFuseImpl::MemInode::find_child_by_name(const char* child_name,
                                                         std::unique_lock<std::mutex>& lock)
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
inline auto MemoryFuseImpl::MemInode::lookup(const char* name)
    -> batt::StatusOr<const fuse_entry_param*>
{
  std::unique_lock<std::mutex> lock{this->mutex_};
  BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> child_inode,
                        this->find_child_by_name(name, lock));

  return {&child_inode->entry_};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto MemoryFuseImpl::MemInode::get_attributes(fuse_req_t /*req*/, fuse_file_info* /*fi*/)
    -> batt::StatusOr<FuseImplBase::Attributes>
{
  return {FuseImplBase::Attributes{
      .attr = &this->entry_.attr,
      .timeout_sec = 0.0,
  }};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status MemoryFuseImpl::MemInode::set_attributes(fuse_req_t req, fuse_file_info* fi,
                                                      struct stat* attr, int to_set)
{
  std::unique_lock<std::mutex> lock{this->mutex_};

  if ((to_set & FUSE_SET_ATTR_MODE) != 0) {
    this->entry_.attr.st_mode = attr->st_mode;
  }
  if ((to_set & FUSE_SET_ATTR_UID) != 0) {
    this->entry_.attr.st_uid = attr->st_uid;
  }
  if ((to_set & FUSE_SET_ATTR_GID) != 0) {
    this->entry_.attr.st_gid = attr->st_gid;
  }
  if ((to_set & FUSE_SET_ATTR_SIZE) != 0) {
    this->entry_.attr.st_size = attr->st_size;
  }
  if ((to_set & FUSE_SET_ATTR_ATIME) != 0) {
    this->entry_.attr.st_atime = attr->st_atime;
  }
  if ((to_set & FUSE_SET_ATTR_MTIME) != 0) {
    this->entry_.attr.st_mtime = attr->st_mtime;
  }
  if ((to_set & FUSE_SET_ATTR_ATIME_NOW) != 0) {
    BATT_REQUIRE_OK(
        batt::status_from_retval(clock_gettime(CLOCK_REALTIME, &this->entry_.attr.st_atim)));
  }
  if ((to_set & FUSE_SET_ATTR_MTIME_NOW) != 0) {
    BATT_REQUIRE_OK(
        batt::status_from_retval(clock_gettime(CLOCK_REALTIME, &this->entry_.attr.st_mtim)));
  }
  if ((to_set & FUSE_SET_ATTR_CTIME) != 0) {
    this->entry_.attr.st_ctime = attr->st_ctime;
  }

  *attr = this->entry_.attr;

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto MemoryFuseImpl::MemInode::pack_as_fuse_dir_entry(
    fuse_req_t req, batt::ConstBuffer& out_buf, batt::MutableBuffer& dst_buf, const char* name,
    DirentOffset offset, PlusApi plus_api, std::unique_lock<std::mutex>& lock) const -> batt::Status
{
  // TODO [tastolfi 2023-06-30] do we need to bump the inode refcount here?
  //
  usize size_needed = [&] {
    LLFS_VLOG(1) << "DEBUG: readdir(): added to buffer: " << name << ", ino "
                 << this->entry_.attr.st_ino << ", offset " << offset;

    if (plus_api) {
      return fuse_add_direntry_plus(req, static_cast<char*>(dst_buf.data()), dst_buf.size(), name,
                                    &this->entry_, offset);
    } else {
      return fuse_add_direntry(req, static_cast<char*>(dst_buf.data()), dst_buf.size(), name,
                               &this->entry_.attr, offset);
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
inline auto MemoryFuseImpl::MemInode::readdir(MemFileHandle& dh, fuse_req_t req, size_t size,
                                              DirentOffset offset, fuse_file_info* fi,
                                              PlusApi plus_api)  //
    -> batt::StatusOr<FuseReadDirData>
{
  std::unique_lock<std::mutex> lock{this->mutex_};

  LLFS_VLOG(1) << "DEBUG: readdir(): started with offset " << offset;

  std::unique_ptr<char[]> storage{new (std::nothrow) char[size]};
  if (!storage) {
    return {batt::status_from_errno(ENOMEM)};
  }
  batt::ConstBuffer out_buf{storage.get(), 0u};
  batt::MutableBuffer dst_buf{storage.get(), size};

  auto children_slice = batt::as_slice(this->children_by_offset_);
  children_slice.advance_begin(offset);

  for (const auto& [child_inode, name] : children_slice) {
    //----- --- -- -  -  -   -
    // TODO [tastolfi 2023-06-30] maybe do something like this?
    //
    // e.attr.st_ino = entry->d_ino;
    // e.attr.st_mode = entry->d_type << 12;
    //----- --- -- -  -  -   -

    batt::Status pack_status = child_inode->pack_as_fuse_dir_entry(
        req, out_buf, dst_buf, name.c_str(), offset, plus_api, lock);

    if (pack_status == batt::StatusCode::kResourceExhausted) {
      break;
    }
    BATT_REQUIRE_OK(pack_status);

    offset = DirentOffset{offset + 1};
  }

  return {FuseReadDirData{OwnedConstBuffer{std::move(storage), out_buf}}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto MemoryFuseImpl::MemInode::add_child_entry(fuse_req_t req, const char* name,
                                                      batt::SharedPtr<MemInode>&& child_inode)
    -> batt::Status
{
  std::unique_lock<std::mutex> lock{this->mutex_};

  this->children_by_name_.emplace(name, child_inode);
  this->children_by_offset_.emplace_back(std::move(child_inode), name);

  return batt::OkStatus();
}

}  //namespace llfs

#endif  // LLFS_MEM_FUSE_HPP
