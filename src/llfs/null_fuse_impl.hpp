//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_NULL_FUSE_IMPL_HPP
#define LLFS_NULL_FUSE_IMPL_HPP

#include <llfs/config.hpp>
//
#include <llfs/fuse.hpp>
#include <llfs/logging.hpp>

#include <batteries/assert.hpp>

namespace llfs {

/** \brief A minimal example of a FuseImpl class.
 */
class NullFuseImpl : public FuseImpl<NullFuseImpl>
{
 public:
  using FuseImpl<NullFuseImpl>::FuseImpl;

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
  template <typename Handler>
  void async_lookup(fuse_req_t req, fuse_ino_t parent, const char* name, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)parent;
    (void)name;

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_entry_param*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 4/44
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
   */ // 5/44
  template <typename Handler>
  void async_get_attributes(fuse_req_t req, fuse_ino_t ino, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseImplBase::Attributes>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 6/44
  template <typename Handler>
  void async_set_attributes(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
                            struct fuse_file_info* fi, Handler&& handler)
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
   */ // 7/44
  template <typename Handler>
  void async_readlink(fuse_req_t req, fuse_ino_t ino, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;

    BATT_FORWARD(handler)(/*link=*/"");
  }

  /** \brief
   */ // 8/44
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
   */ // 9/44
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
   */ // 10/44
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
   */ // 11/44
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
   */ // 12/44
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
   */ // 13/44
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
   */ // 14/44
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
   */ // 15/44
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
   */ // 16/44
  template <typename Handler>
  void async_read(fuse_req_t req, fuse_ino_t ino, size_t size, FileOffset off, FuseFileHandle fh,
                  Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)size;
    (void)off;
    (void)fh;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseReadData>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 17/44
  template <typename Handler>
  void async_write(fuse_req_t req, fuse_ino_t ino, const batt::ConstBuffer& buffer,
                   FileOffset offset, FuseFileHandle fh, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)buffer;
    (void)offset;
    (void)fh;

    BATT_FORWARD(handler)(batt::StatusOr<usize>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 18/44
  template <typename Handler>
  void async_flush(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fh;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */ // 19/44
  template <typename Handler>
  void async_release(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh, FileOpenFlags flags,
                     Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fh;
    (void)flags;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */ // 20/44
  template <typename Handler>
  void async_fsync(fuse_req_t req, fuse_ino_t ino, IsDataSync datasync, FuseFileHandle fh,
                   Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)datasync;
    (void)fh;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */ // 21/44
  template <typename Handler>
  void async_opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<const fuse_file_info*>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 22/44
  template <typename Handler>
  void async_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, DirentOffset off,
                     FuseFileHandle fh, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)size;
    (void)off;
    (void)fh;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseReadDirData>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 23/44
  template <typename Handler>
  void async_releasedir(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fh;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */ // 24/44
  template <typename Handler>
  void async_fsyncdir(fuse_req_t req, fuse_ino_t ino, IsDataSync datasync, FuseFileHandle fh,
                      Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)datasync;
    (void)fh;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */ // 25/44
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
   */ // 26/44
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
   */ // 27/44
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
   */ // 29/44
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
   */ // 30/44
  template <typename Handler>
  void async_check_access(fuse_req_t req, fuse_ino_t ino, int mask, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)mask;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
  }

  /** \brief
   */ // 31/44
  template <typename Handler>
  void async_create(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
                    fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)parent;
    (void)name;
    (void)mode;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseImplBase::FuseCreateReply>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 37/44
  template <typename Handler>
  void async_write_buf(fuse_req_t req, fuse_ino_t ino, const FuseImplBase::ConstBufferVec& bufv,
                       FileOffset offset, FuseFileHandle fh, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)bufv;
    (void)offset;
    (void)fh;

    BATT_FORWARD(handler)(batt::StatusOr<usize>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */ // 38/44
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
   */ // 39/44
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
   */ // 41/44
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
   */ // 42/44
  template <typename Handler>
  void async_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, DirentOffset offset,
                         FuseFileHandle fh, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)size;
    (void)offset;
    (void)fh;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseReadDirData>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

};  // class NullFuseImpl

}  //namespace llfs

#endif  // LLFS_NULL_FUSE_IMPL_HPP
