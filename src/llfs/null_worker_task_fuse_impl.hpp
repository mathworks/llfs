//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_NULL_WORKER_TASK_FUSE_IMPL_HPP
#define LLFS_NULL_WORKER_TASK_FUSE_IMPL_HPP

#include <llfs/config.hpp>
//
#include <llfs/fuse.hpp>

#include <batteries/suppress.hpp>

namespace llfs {

BATT_SUPPRESS_IF_GCC("-Wunused-parameter")

class NullWorkerTaskFuseImpl : public WorkerTaskFuseImpl<NullWorkerTaskFuseImpl>
{
 public:
  using Super = WorkerTaskFuseImpl<NullWorkerTaskFuseImpl>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit NullWorkerTaskFuseImpl(std::shared_ptr<WorkQueue>&& work_queue) noexcept
      : Super{std::move(work_queue)}
  {
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
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 4/44
  void forget_inode(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
  {
  }

  /** \brief
   */ // 5/44
  batt::StatusOr<FuseImplBase::Attributes> get_attributes(fuse_req_t req, fuse_ino_t ino,
                                                          fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 6/44
  batt::StatusOr<FuseImplBase::Attributes> set_attributes(fuse_req_t req, fuse_ino_t ino,
                                                          struct stat* attr, int to_set,
                                                          fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
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
                                                    const std::string& name, mode_t mode,
                                                    dev_t rdev)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 9/44
  batt::StatusOr<const fuse_entry_param*> make_directory(fuse_req_t req, fuse_ino_t parent,
                                                         const std::string& name, mode_t mode)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 10/44
  batt::Status unlink(fuse_req_t req, fuse_ino_t parent, const std::string& name)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 11/44
  batt::Status remove_directory(fuse_req_t req, fuse_ino_t parent, const std::string& name)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 12/44
  batt::StatusOr<const fuse_entry_param*> symbolic_link(fuse_req_t req, const std::string& link,
                                                        fuse_ino_t parent, const std::string& name)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 13/44
  batt::Status rename(fuse_req_t req, fuse_ino_t parent, const std::string& name,
                      fuse_ino_t newparent, const std::string& newname, unsigned int flags)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 14/44
  batt::StatusOr<const fuse_entry_param*> hard_link(fuse_req_t req, fuse_ino_t ino,
                                                    fuse_ino_t newparent,
                                                    const std::string& newname)
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
  batt::Status flush(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 19/44
  batt::Status release(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
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
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 22/44
  batt::StatusOr<FuseImplBase::FuseReadDirData> readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                                                        DirentOffset off, fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
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
      fuse_req_t req, fuse_ino_t ino, const std::string& name, size_t size)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 29/44
  batt::Status remove_extended_attribute(fuse_req_t req, fuse_ino_t ino, const std::string& name)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 30/44
  batt::Status check_access(fuse_req_t req, fuse_ino_t ino, int mask)
  {
    return {batt::StatusCode::kUnimplemented};
  }

  /** \brief
   */ // 31/44
  batt::StatusOr<FuseImplBase::FuseCreateReply> create(fuse_req_t req, fuse_ino_t parent,
                                                       const std::string& name, mode_t mode,
                                                       fuse_file_info* fi)
  {
    return {batt::StatusCode::kUnimplemented};
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
    return {batt::StatusCode::kUnimplemented};
  }

};  // class NullWorkerTaskFuseImpl

BATT_UNSUPPRESS_IF_GCC()

}  //namespace llfs

#endif  // LLFS_NULL_WORKER_TASK_FUSE_IMPL_HPP
