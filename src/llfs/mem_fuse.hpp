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
#include <llfs/mem_file_handle.hpp>
#include <llfs/mem_inode.hpp>
#include <llfs/worker_task_fuse_impl.hpp>

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
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<fuse_ino_t> next_unused_ino_{FUSE_ROOT_ID + 1};
  std::atomic<u64> next_unused_fh_int_{3};

  struct State {
    std::unordered_map<fuse_ino_t, batt::SharedPtr<MemInode>> inodes_;

    std::unordered_map<FuseFileHandle, batt::SharedPtr<MemFileHandle>, std::hash<u64>>
        file_handles_;

    std::vector<FuseFileHandle> available_fhs_;
  };

  batt::Mutex<State> state_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemoryFuseImpl(std::shared_ptr<WorkQueue>&& work_queue) noexcept
      : WorkerTaskFuseImpl<MemoryFuseImpl>{std::move(work_queue)}
  {
    auto locked = this->state_.lock();

    locked->inodes_.emplace(
        FUSE_ROOT_ID,
        batt::make_shared<MemInode>(FUSE_ROOT_ID, MemInode::Category::kDirectory, /*mode=*/0755));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief
   */ // 1/44
  void init()
  {
    BATT_CHECK_NOT_NULLPTR(this->conn_);
  }

  /** \brief
   */ // 2/44
  void destroy()
  {
    BATT_CHECK_NOT_NULLPTR(this->conn_);
  }

  /** \brief
   */ // 3/44
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

    return parent_inode->lookup_child(name);
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
   */ // 5/44
  batt::StatusOr<FuseImplBase::Attributes> get_attributes(fuse_req_t req, fuse_ino_t ino)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return inode->get_attributes();
  }

  /** \brief
   */ // 6/44
  batt::StatusOr<FuseImplBase::Attributes> set_attributes(fuse_req_t req, fuse_ino_t ino,
                                                          const struct stat* attr, int to_set,
                                                          batt::Optional<FuseFileHandle> fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__                 //
                 << "(" << BATT_INSPECT(req)                           //
                 << "," << BATT_INSPECT(ino)                           //
                 << "," << BATT_INSPECT(DumpStat{*attr})               //
                 << "," << BATT_INSPECT(std::bitset<17>{(u32)to_set})  //
                 << "," << BATT_INSPECT(fh)                            //
                 << " )";
    /*
     * If the setattr was invoked from the ftruncate() system call
     * under Linux kernel versions 2.6.15 or later, the fi->fh will
     * contain the value set by the open method or will be undefined
     * if the open method didn't set any value.  Otherwise (not
     * ftruncate call, or kernel version earlier than 2.6.15) the fi
     * parameter will be NULL.
     */

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return inode->set_attributes(attr, to_set);
  }

  /** \brief
   */ // 7/44
  const char* readlink(fuse_req_t req, fuse_ino_t ino)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << "("                                 //
                     << " )"
                     << " NOT IMPLEMENTED";

    return "";
  }

  /** \brief
   */ // 8/44
  batt::StatusOr<const fuse_entry_param*> make_node(fuse_req_t req, fuse_ino_t parent,
                                                    const std::string& name, mode_t mode,
                                                    dev_t rdev)
  {
    LLFS_LOG_WARNING() << "MemoryFuseImpl::" << __FUNCTION__  //
                       << "("                                 //
                       << " )"
                       << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 9/44
  batt::StatusOr<const fuse_entry_param*> make_directory(fuse_req_t req, fuse_ino_t parent,
                                                         const std::string& name, mode_t mode)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__       //
                 << "(" << BATT_INSPECT(req)                 //
                 << "," << BATT_INSPECT(parent)              //
                 << "," << BATT_INSPECT_STR(name)            //
                 << "," << BATT_INSPECT(DumpFileMode{mode})  //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> new_inode,
                          this->create_inode_impl(parent, name, mode, MemInode::IsDir{true}));

    return {new_inode->get_fuse_entry_param()};
  }

  /** \brief
   */ // 10/44
  batt::Status unlink(fuse_req_t req, fuse_ino_t parent, const std::string& name)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(parent)         //
                 << "," << BATT_INSPECT_STR(name)       //
                 << " )";
    ;

    return this->unlink_impl(req, parent, name, MemInode::IsDir{false});
  }

  /** \brief
   */ // 11/44
  batt::Status remove_directory(fuse_req_t req, fuse_ino_t parent, const std::string& name)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(parent)         //
                 << "," << BATT_INSPECT_STR(name)       //
                 << " )";

    return this->unlink_impl(req, parent, name, MemInode::IsDir{true});
  }

  /** \brief
   */ // 12/44
  batt::StatusOr<const fuse_entry_param*> symbolic_link(fuse_req_t req, const std::string& link,
                                                        fuse_ino_t parent, const std::string& name)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << "("                                 //
                     << " )"
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 13/44
  batt::Status rename(fuse_req_t req, fuse_ino_t parent, const std::string& name,
                      fuse_ino_t newparent, const std::string& newname, unsigned int flags)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << "("                                 //
                     << " )"
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
                     << "("                                 //
                     << " )"
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 15/44
  batt::StatusOr<const fuse_file_info*> open(fuse_req_t req, fuse_ino_t ino,
                                             const fuse_file_info& fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(fi)             //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));
    BATT_ASSIGN_OK_RESULT(const fuse_file_info* ofi,
                          this->open_inode_impl(batt::make_copy(inode), fi));

    inode->add_lookup(1);

    return {ofi};
  }

  /** \brief
   */ // 16/44
  batt::StatusOr<FuseImplBase::FuseReadData> read(fuse_req_t req, fuse_ino_t ino, size_t size,
                                                  FileOffset offset, FuseFileHandle fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(size)           //
                 << "," << BATT_INSPECT(offset)         //
                 << "," << BATT_INSPECT(fh)             //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));

    return {FuseImplBase::FuseReadData{inode->read(offset, size)}};
  }

  /** \brief
   */ // 17/44
  batt::StatusOr<usize> write(fuse_req_t req, fuse_ino_t ino, const batt::ConstBuffer& buffer,
                              FileOffset offset, FuseFileHandle fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(buffer.size())  //
                 << "," << BATT_INSPECT(offset)         //
                 << "," << BATT_INSPECT(fh)             //
                 << " )";

    return this->write_impl(req, ino, batt::as_slice(&buffer, 1), offset, fh);
  }

  /** \brief
   */ // 37/44
  batt::StatusOr<usize> write_buf(fuse_req_t req, fuse_ino_t ino,
                                  const FuseImplBase::ConstBufferVec& bufv, FileOffset offset,
                                  FuseFileHandle fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(offset)         //
                 << "," << BATT_INSPECT(fh)             //
                 << " )";

    return this->write_impl(req, ino, batt::as_slice(bufv), offset, fh);
  }

  /** \brief
   */ // 18/44
  batt::Status flush(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(fh)             //
                 << " )";

    return batt::OkStatus();
  }

  /** \brief
   */ // 19/44
  batt::Status release(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh, FileOpenFlags flags)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(fh)             //
                 << "," << BATT_INSPECT(flags)          //
                 << " )";

    return this->close_impl(fh);
  }

  /** \brief
   */ // 20/44
  batt::Status fsync(fuse_req_t req, fuse_ino_t ino, IsDataSync datasync, FuseFileHandle fh)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << "("                                 //
                     << " )"
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 21/44
  batt::StatusOr<const fuse_file_info*> opendir(fuse_req_t req, fuse_ino_t ino,
                                                const fuse_file_info& fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(fi)             //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> inode, this->find_inode(ino));
    BATT_ASSIGN_OK_RESULT(
        const fuse_file_info* ofi,
        this->open_inode_impl(batt::make_copy(inode), fi, MemFileHandle::OpenDirState{}));

    inode->add_lookup(1);

    return {ofi};
  }

  /** \brief
   */ // 22/44
  batt::StatusOr<FuseImplBase::FuseReadDirData> readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                                                        DirentOffset offset, FuseFileHandle fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(size)           //
                 << "," << BATT_INSPECT(offset)         //
                 << "," << BATT_INSPECT(fh)             //
                 << " )";

    return this->readdir_impl(req, ino, size, offset, fh, PlusApi{false});
  }

  /** \brief
   */ // 23/44
  batt::Status releasedir(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(fh)             //
                 << " )";

    return this->close_impl(fh);
  }

  /** \brief
   */ // 24/44
  batt::Status fsyncdir(fuse_req_t req, fuse_ino_t ino, IsDataSync datasync, FuseFileHandle fh)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << "("                                 //
                     << " )"
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 25/44
  batt::StatusOr<const struct statvfs*> statfs(fuse_req_t req, fuse_ino_t ino)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << "("                                 //
                     << " )"
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 26/44
  batt::Status set_extended_attribute(fuse_req_t req, fuse_ino_t ino,
                                      const FuseImplBase::ExtendedAttribute& attr, int flags)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__  //
                     << "("                                 //
                     << " )"
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 27/44
  batt::StatusOr<FuseImplBase::FuseGetExtendedAttributeReply> get_extended_attribute(
      fuse_req_t req, fuse_ino_t ino, const std::string& name, size_t size)
  {
    LLFS_LOG_WARNING() << "MemoryFuseImpl::" << __FUNCTION__  //
                       << "("                                 //
                       << " )"
                       << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 29/44
  batt::Status remove_extended_attribute(fuse_req_t req, fuse_ino_t ino, const std::string& name)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__ << "("  //
                     << " )"
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
                                                       const fuse_file_info& fi)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__       //
                 << "(" << BATT_INSPECT(req)                 //
                 << "," << BATT_INSPECT(parent)              //
                 << "," << BATT_INSPECT_STR(name)            //
                 << "," << BATT_INSPECT(DumpFileMode{mode})  //
                 << "," << BATT_INSPECT(fi)                  //
                 << " )";

    BATT_ASSIGN_OK_RESULT(batt::SharedPtr<MemInode> new_inode,
                          this->create_inode_impl(parent, name, mode, MemInode::IsDir{false}));

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
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__ << "("  //
                     << " )"
                     << " NOT IMPLEMENTED";
  }

  /** \brief
   */ // 39/44
  void forget_multiple_inodes(fuse_req_t req, batt::Slice<fuse_forget_data> forgets)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__ << "("  //
                     << " )"
                     << " NOT IMPLEMENTED";
  }

  /** \brief
   */ // 41/44
  batt::Status file_allocate(fuse_req_t req, fuse_ino_t ino, int mode, FileOffset offset,
                             FileLength length, fuse_file_info* fi)
  {
    LLFS_LOG_ERROR() << "MemoryFuseImpl::" << __FUNCTION__ << "("  //
                     << " )"
                     << " NOT IMPLEMENTED";

    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 42/44
  batt::StatusOr<FuseImplBase::FuseReadDirData> readdirplus(fuse_req_t req, fuse_ino_t ino,
                                                            size_t size, DirentOffset offset,
                                                            FuseFileHandle fh)
  {
    LLFS_VLOG(1) << "MemoryFuseImpl::" << __FUNCTION__  //
                 << "(" << BATT_INSPECT(req)            //
                 << "," << BATT_INSPECT(ino)            //
                 << "," << BATT_INSPECT(size)           //
                 << "," << BATT_INSPECT(offset)         //
                 << "," << BATT_INSPECT(fh)             //
                 << " )";

    return this->readdir_impl(req, ino, size, offset, fh, PlusApi{true});
  }

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns an unused inode (ino) integer.
   */
  fuse_ino_t allocate_ino_int();

  /** \brief Returns an unused file handle integer.
   */
  FuseFileHandle allocate_fh_int();

  /** \brief
   */
  batt::StatusOr<batt::SharedPtr<MemInode>> find_inode(fuse_ino_t ino);

  /** \brief
   */
  batt::StatusOr<batt::SharedPtr<MemFileHandle>> find_file_handle(FuseFileHandle fh);

  /** \brief
   */
  batt::StatusOr<batt::SharedPtr<MemInode>> create_inode_impl(fuse_ino_t parent,
                                                              const std::string& name, mode_t mode,
                                                              MemInode::IsDir is_dir);

  /** \brief
   */
  template <typename... OpenStateArgs>
  batt::StatusOr<const fuse_file_info*> open_inode_impl(batt::SharedPtr<MemInode>&& inode,
                                                        const fuse_file_info& fi,
                                                        OpenStateArgs&&... open_state_args);

  /** \brief
   */
  batt::Status close_impl(FuseFileHandle fh);

  /** \brief
   */
  batt::StatusOr<FuseReadDirData> readdir_impl(fuse_req_t req, fuse_ino_t ino, size_t size,
                                               DirentOffset offset, FuseFileHandle fh,
                                               PlusApi plus_api);

  /** \brief
   */
  batt::Status unlink_impl(fuse_req_t req, fuse_ino_t parent, const std::string& name,
                           MemInode::IsDir is_dir);

  /** \brief
   */
  batt::StatusOr<usize> write_impl(fuse_req_t req, fuse_ino_t ino,
                                   const batt::Slice<const batt::ConstBuffer>& buffers,
                                   FileOffset offset, FuseFileHandle fh);

};  // class MemoryFuseImpl

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  //namespace llfs

#include <llfs/mem_fuse.ipp>

#endif  // LLFS_MEM_FUSE_HPP
