//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FUSE_HPP
#define LLFS_FUSE_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/logging.hpp>
#include <llfs/worker_task.hpp>

#include <batteries/buffer.hpp>
#include <batteries/optional.hpp>
#include <batteries/pointers.hpp>
#include <batteries/slice.hpp>
#include <batteries/status.hpp>

#include <fuse_lowlevel.h>

#include <bitset>
#include <string_view>

#include <errno.h>
#include <sys/statvfs.h>

namespace llfs {

std::ostream& operator<<(std::ostream& out, const fuse_file_info& t);

std::ostream& operator<<(std::ostream& out, const fuse_file_info* t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct DumpStat {
  const struct stat& s;
};

std::ostream& operator<<(std::ostream& out, const DumpStat& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct DumpFileMode {
  explicit DumpFileMode(mode_t mode) noexcept : mode{mode}
  {
  }

  explicit DumpFileMode(int mode) noexcept : mode{(mode_t)mode}
  {
  }

  mode_t mode;
};

std::ostream& operator<<(std::ostream& out, const DumpFileMode t);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FuseImplBase
{
 public:
  struct FileDataRef {
    FileDescriptorInt fd;
    batt::Optional<FileOffset> offset;
    usize size;
    bool should_retry;
  };

  struct OwnedConstBuffer {
    std::unique_ptr<char[]> storage;
    batt::ConstBuffer buffer;
  };

  struct OwnedMutableBuffer {
    std::unique_ptr<char[]> storage;
    batt::ConstBuffer buffer;
  };

  using FuseConstBuffer = std::variant<batt::ConstBuffer, OwnedConstBuffer, FileDataRef>;
  using FuseMutableBuffer = std::variant<batt::MutableBuffer, OwnedMutableBuffer, FileDataRef>;

  struct FuseConstBufferVec {
    usize current_buffer_index;
    usize current_buffer_offset;
    batt::Slice<FuseConstBuffer> buffers;
  };

  struct FuseMutableBufferVec {
    usize current_buffer_index;
    usize current_buffer_offset;
    batt::Slice<FuseMutableBuffer> buffers;
  };

  using FuseReadData = std::variant<   //
      batt::ConstBuffer,               //
      OwnedConstBuffer,                //
      batt::Slice<batt::ConstBuffer>,  //
      FuseConstBufferVec               //
      >;

  using FuseReadDirData = std::variant<  //
      batt::ConstBuffer,                 //
      OwnedConstBuffer,                  //
      FuseConstBufferVec                 //
      >;

  struct Attributes {
    const struct stat* attr;
    double timeout_sec;
  };

  struct ExtendedAttribute {
    std::string_view name;
    batt::ConstBuffer value;
  };

  using FuseGetExtendedAttributeReply = std::variant<  //
      batt::ConstBuffer,                               //
      OwnedConstBuffer,                                //
      FuseConstBufferVec,                              //
      BufferSizeNeeded                                 //
      >;

  struct FuseCreateReply {
    const fuse_entry_param* entry;
    const fuse_file_info* fi;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief
   */
  static constexpr bool can_cast_iovec_to_const_buffer()
  {
    return sizeof(struct iovec) == sizeof(batt::ConstBuffer);
  }

  /** \brief
   */
  static int errno_from_status(batt::Status status);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  FuseImplBase() = default;

  FuseImplBase(const FuseImplBase&) = delete;
  FuseImplBase& operator=(const FuseImplBase&) = delete;

  virtual ~FuseImplBase() = default;

  /** \brief
   */
  auto make_error_handler(fuse_req_t req)
  {
    return [req](batt::Status result) {
      fuse_reply_err(req, FuseImplBase::errno_from_status(result));
    };
  }

  /** \brief
   */
  auto make_entry_handler(fuse_req_t req)
  {
    return [req](batt::StatusOr<const fuse_entry_param*> result) {
      if (!result.ok()) {
        LLFS_VLOG(1) << BATT_INSPECT(req) << BATT_INSPECT(result.status());
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        BATT_CHECK_NOT_NULLPTR(*result);
        LLFS_VLOG(1) << BATT_INSPECT(req) << " OK" << BATT_INSPECT(*result);
        fuse_reply_entry(req, *result);
      }
    };
  }

  /** \brief
   */
  auto make_attributes_handler(fuse_req_t req)
  {
    return [req](batt::StatusOr<Attributes> result) {
      if (!result.ok()) {
        LLFS_VLOG(1) << BATT_INSPECT(req) << BATT_INSPECT(result.status());
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        BATT_CHECK_NOT_NULLPTR(result->attr);
        LLFS_VLOG(1) << BATT_INSPECT(req) << " OK" << BATT_INSPECT(DumpStat{*result->attr});
        fuse_reply_attr(req, result->attr, result->timeout_sec);
      }
    };
  }

  /** \brief
   */
  auto make_readlink_handler(fuse_req_t req)
  {
    return [req](const char* link) {
      fuse_reply_readlink(req, link);
    };
  }

  /** \brief
   */
  auto make_open_handler(fuse_req_t req)
  {
    return [req](batt::StatusOr<const fuse_file_info*> result) {
      if (!result.ok()) {
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        fuse_reply_open(req, *result);
      }
    };
  }

  /** \brief
   */
  auto make_create_handler(fuse_req_t req)
  {
    return [req](const batt::StatusOr<FuseCreateReply>& result) {
      if (!result.ok()) {
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        fuse_reply_create(req, result->entry, result->fi);
      }
    };
  }

  /** \brief
   */
  auto make_read_handler(fuse_req_t req)
  {
    return [req, this](const batt::StatusOr<FuseImplBase::FuseReadData>& result) {
      if (!result.ok()) {
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        batt::case_of(  //
            *result,    //

            //----- --- -- -  -  -   -
            [&req, this](const batt::ConstBuffer& cb) {
              this->invoke_fuse_reply_buf(req, cb);
            },

            //----- --- -- -  -  -   -
            [&req, this](const OwnedConstBuffer& ocb) {
              this->invoke_fuse_reply_buf(req, ocb.buffer);
            },

            //----- --- -- -  -  -   -
            [&req, this](const batt::Slice<batt::ConstBuffer>& cbs) {
              this->invoke_fuse_reply_iov(req, cbs);
            },

            //----- --- -- -  -  -   -
            [&req, this](const FuseConstBufferVec& v) {
              this->invoke_fuse_reply_data(req, v);
            });
      }
    };
  }

  /** \brief
   */
  auto make_readdir_handler(fuse_req_t req)
  {
    return [req, this](const batt::StatusOr<FuseImplBase::FuseReadDirData>& result) {
      if (!result.ok()) {
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        batt::case_of(  //
            *result,    //

            //----- --- -- -  -  -   -
            [&req, this](const batt::ConstBuffer& cb) {
              this->invoke_fuse_reply_buf(req, cb);
            },

            //----- --- -- -  -  -   -
            [&req, this](const OwnedConstBuffer& ocb) {
              this->invoke_fuse_reply_buf(req, ocb.buffer);
            },

            //----- --- -- -  -  -   -
            [&req, this](const FuseConstBufferVec& v) {
              this->invoke_fuse_reply_data(req, v);
            });
      }
    };
  }

  /** \brief
   */
  auto make_write_handler(fuse_req_t req)
  {
    return [req](batt::StatusOr<usize> result) {
      if (!result.ok()) {
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        fuse_reply_write(req, /*count=*/*result);
      }
    };
  }

  /** \brief
   */
  auto make_statfs_handler(fuse_req_t req)
  {
    return [req](batt::StatusOr<const struct statvfs*> result) {
      if (!result.ok()) {
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        fuse_reply_statfs(req, /*stbuf=*/*result);
      }
    };
  }

  /** \brief
   */
  auto make_extended_attribute_handler(fuse_req_t req)
  {
    return [req, this](const batt::StatusOr<FuseGetExtendedAttributeReply>& result) {
      if (!result.ok()) {
        fuse_reply_err(req, FuseImplBase::errno_from_status(result.status()));
      } else {
        batt::case_of(  //
            *result,    //

            //----- --- -- -  -  -   -
            [&req, this](const batt::ConstBuffer& cb) {
              this->invoke_fuse_reply_buf(req, cb);
            },

            //----- --- -- -  -  -   -
            [&req, this](const OwnedConstBuffer& ocb) {
              this->invoke_fuse_reply_buf(req, ocb.buffer);
            },

            //----- --- -- -  -  -   -
            [&req, this](const FuseConstBufferVec& v) {
              this->invoke_fuse_reply_data(req, v);
            },

            //----- --- -- -  -  -   -
            [&req](BufferSizeNeeded count) {
              fuse_reply_xattr(req, count.value());
            });
      }
    };
  }

  /** \brief
   */
  auto make_no_arg_handler(fuse_req_t req)
  {
    return [req]() {
      fuse_reply_none(req);
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief
   */
  int invoke_fuse_reply_buf(fuse_req_t req, const batt::ConstBuffer& cb);

  /** \brief
   */
  int invoke_fuse_reply_iov(fuse_req_t req, const batt::Slice<batt::ConstBuffer>& cbs);

  /** \brief
   */
  int invoke_fuse_reply_data(fuse_req_t req, const FuseConstBufferVec& v);

 protected:
  fuse_conn_info* conn_ = nullptr;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief
 */
template <typename Derived>
class FuseImpl : public FuseImplBase
{
 public:
  using Self = FuseImpl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  //----- --- -- -  -  -   -
  // FUSE op impls.
  //----- --- -- -  -  -   -

  static void op_init_impl(void* userdata, struct fuse_conn_info* conn);

  static void op_destroy_impl(void* userdata);

  static void op_lookup_impl(fuse_req_t req, fuse_ino_t parent, const char* name);

  static void op_forget_impl(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup);

  static void op_getattr_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void op_setattr_impl(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
                              struct fuse_file_info* fi);

  static void op_readlink_impl(fuse_req_t req, fuse_ino_t ino);

  static void op_mknod_impl(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
                            dev_t rdev);

  static void op_mkdir_impl(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode);

  static void op_unlink_impl(fuse_req_t req, fuse_ino_t parent, const char* name);

  static void op_rmdir_impl(fuse_req_t req, fuse_ino_t parent, const char* name);

  static void op_symlink_impl(fuse_req_t req, const char* link, fuse_ino_t parent,
                              const char* name);

  static void op_rename_impl(fuse_req_t req, fuse_ino_t parent, const char* name,
                             fuse_ino_t newparent, const char* newname, unsigned int flags);

  static void op_link_impl(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                           const char* newname);

  static void op_open_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void op_read_impl(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                           struct fuse_file_info* fi);

  static void op_write_impl(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size, off_t off,
                            struct fuse_file_info* fi);

  static void op_flush_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void op_release_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void op_fsync_impl(fuse_req_t req, fuse_ino_t ino, int datasync,
                            struct fuse_file_info* fi);

  static void op_opendir_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void op_readdir_impl(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                              struct fuse_file_info* fi);

  static void op_releasedir_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void op_fsyncdir_impl(fuse_req_t req, fuse_ino_t ino, int datasync,
                               struct fuse_file_info* fi);

  static void op_statfs_impl(fuse_req_t req, fuse_ino_t ino);

  static void op_setxattr_impl(fuse_req_t req, fuse_ino_t ino, const char* name, const char* value,
                               size_t size, int flags);

  static void op_getxattr_impl(fuse_req_t req, fuse_ino_t ino, const char* name, size_t size);

  static void op_listxattr_impl(fuse_req_t req, fuse_ino_t ino, size_t size);

  static void op_removexattr_impl(fuse_req_t req, fuse_ino_t ino, const char* name);

  static void op_access_impl(fuse_req_t req, fuse_ino_t ino, int mask);

  static void op_create_impl(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
                             struct fuse_file_info* fi);

  static void op_getlk_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                            struct flock* lock);

  static void op_setlk_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                            struct flock* lock, int sleep);

  static void op_bmap_impl(fuse_req_t req, fuse_ino_t ino, size_t blocksize, uint64_t idx);

  static void op_ioctl_impl(fuse_req_t req, fuse_ino_t ino, unsigned int cmd, void* arg,
                            struct fuse_file_info* fi, unsigned flags, const void* in_buf,
                            size_t in_bufsz, size_t out_bufsz);

  static void op_poll_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                           struct fuse_pollhandle* ph);

  static void op_write_buf_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_bufvec* bufv, off_t off,
                                struct fuse_file_info* fi);

  static void op_retrieve_reply_impl(fuse_req_t req, void* cookie, fuse_ino_t ino, off_t offset,
                                     struct fuse_bufvec* bufv);

  static void op_forget_multi_impl(fuse_req_t req, size_t count, struct fuse_forget_data* forgets);

  static void op_flock_impl(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi, int op);

  static void op_fallocate_impl(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset,
                                off_t length, struct fuse_file_info* fi);

  static void op_readdirplus_impl(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                                  struct fuse_file_info* fi);

  static void op_copy_file_range_impl(fuse_req_t req, fuse_ino_t ino_in, off_t off_in,
                                      struct fuse_file_info* fi_in, fuse_ino_t ino_out,
                                      off_t off_out, struct fuse_file_info* fi_out, size_t len,
                                      int flags);

  static void op_lseek_impl(fuse_req_t req, fuse_ino_t ino, off_t off, int whence,
                            struct fuse_file_info* fi);

  //----- --- -- -  -  -   -

  static const fuse_lowlevel_ops* get_fuse_lowlevel_ops();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  FuseImpl() = default;

  Derived* derived_this() noexcept
  {
    return static_cast<Derived*>(this);
  }

  const Derived* derived_this() const noexcept
  {
    return static_cast<const Derived*>(this);
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief A minimal example of a FuseImpl class.
 */
class NullFuseImpl : public FuseImpl<NullFuseImpl>
{
 public:
  using FuseImpl<NullFuseImpl>::FuseImpl;

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
  template <typename Handler>
  void async_get_attributes(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                            Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseImplBase::Attributes>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

  /** \brief
   */
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
  void async_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, DirentOffset off,
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
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)mask;

    BATT_FORWARD(handler)(batt::Status{batt::StatusCode::kUnimplemented});
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
  void async_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, DirentOffset offset,
                         fuse_file_info* fi, Handler&& handler)
  {
    LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;

    (void)req;
    (void)ino;
    (void)size;
    (void)offset;
    (void)fi;

    BATT_FORWARD(handler)
    (batt::StatusOr<FuseReadDirData>{batt::Status{batt::StatusCode::kUnimplemented}});
  }

};  // class NullFuseImpl

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename Derived>
class WorkerTaskFuseImpl : public FuseImpl<Derived>
{
 public:
  explicit WorkerTaskFuseImpl(std::shared_ptr<WorkQueue>&& work_queue) noexcept
      : work_queue_{std::move(work_queue)}
  {
  }

  /** \brief
   */ // 3/44
  template <typename Handler>
  void async_lookup(fuse_req_t req, fuse_ino_t parent, const char* name, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(parent)
                 << BATT_INSPECT(name);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, parent, name, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->lookup(req, parent, name));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_entry_param*>{push_status});
    }
  }

  /** \brief
   */ // 4/44
  template <typename Handler>
  void async_forget_inode(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(nlookup);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, nlookup, handler = BATT_FORWARD(handler)] {
          auto on_scope_exit = batt::finally([&] {
            BATT_FORWARD(handler)();
          });
          this->derived_this()->forget_inode(req, ino, nlookup);
        });

    if (!push_status.ok()) {
      LLFS_LOG_WARNING() << "Could not push request to work queue;" << BATT_INSPECT(push_status);
      BATT_FORWARD(handler)();
    }
  }

  /** \brief
   */ // 5/44
  template <typename Handler>
  void async_get_attributes(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                            Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->get_attributes(req, ino, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::Attributes>{push_status});
    }
  }

  /** \brief
   */ // 6/44
  template <typename Handler>
  void async_set_attributes(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
                            fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION    //
                 << BATT_INSPECT(req)     //
                 << BATT_INSPECT(ino)     //
                 << BATT_INSPECT(attr)    //
                 << BATT_INSPECT(to_set)  //
                 << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, attr, to_set, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->set_attributes(req, ino, attr, to_set, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::Attributes>{push_status});
    }
  }

  /** \brief
   */ // 7/44
  template <typename Handler>
  void async_readlink(fuse_req_t req, fuse_ino_t ino, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->readlink(req, ino));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(/*link=*/"");
    }
  }

  /** \brief
   */ // 8/44
  template <typename Handler>
  void async_make_node(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode, dev_t rdev,
                       Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION                //
                 << BATT_INSPECT(req)                 //
                 << BATT_INSPECT(parent)              //
                 << BATT_INSPECT(name)                //
                 << BATT_INSPECT(DumpFileMode{mode})  //
                 << BATT_INSPECT(rdev);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, parent, name, mode, rdev, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->make_node(req, parent, name, mode, rdev));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_entry_param*>{push_status});
    }
  }

  /** \brief
   */ // 9/44
  template <typename Handler>
  void async_make_directory(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
                            Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(parent)
                 << BATT_INSPECT(name) << BATT_INSPECT(DumpFileMode{mode});

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, parent, name, mode, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->make_directory(req, parent, name, mode));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_entry_param*>{push_status});
    }
  }

  /** \brief
   */ // 10/44
  template <typename Handler>
  void async_unlink(fuse_req_t req, fuse_ino_t parent, const char* name, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(parent)
                 << BATT_INSPECT(name);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, parent, name, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->unlink(req, parent, name));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 11/44
  template <typename Handler>
  void async_remove_directory(fuse_req_t req, fuse_ino_t parent, const char* name,
                              Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(parent)
                 << BATT_INSPECT(name);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, parent, name, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->remove_directory(req, parent, name));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 12/44
  template <typename Handler>
  void async_symbolic_link(fuse_req_t req, const char* link, fuse_ino_t parent, const char* name,
                           Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(link)
                 << BATT_INSPECT(parent) << BATT_INSPECT(name);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, link, parent, name, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->symbolic_link(req, link, parent, name));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_entry_param*>{push_status});
    }
  }

  /** \brief
   */ // 13/44
  template <typename Handler>
  void async_rename(fuse_req_t req, fuse_ino_t parent, const char* name, fuse_ino_t newparent,
                    const char* newname, unsigned int flags, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(parent)
                 << BATT_INSPECT(name) << BATT_INSPECT(newparent) << BATT_INSPECT(newname)
                 << BATT_INSPECT(flags);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, parent, name, newparent, newname, flags, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->rename(req, parent, name, newparent, newname, flags));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 14/44
  template <typename Handler>
  void async_hard_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char* newname,
                       Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(newparent) << BATT_INSPECT(newname);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, newparent, newname, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->hard_link(req, ino, newparent, newname));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_entry_param*>{push_status});
    }
  }

  /** \brief
   */ // 15/44
  template <typename Handler>
  void async_open(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->open(req, ino, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_file_info*>{push_status});
    }
  }

  /** \brief
   */ // 16/44
  template <typename Handler>
  void async_read(fuse_req_t req, fuse_ino_t ino, size_t size, FileOffset offset,
                  fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(size) << BATT_INSPECT(offset) << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, size, offset, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->read(req, ino, size, offset, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::FuseReadData>{push_status});
    }
  }

  /** \brief
   */ // 17/44
  template <typename Handler>
  void async_write(fuse_req_t req, fuse_ino_t ino, const batt::ConstBuffer& buffer,
                   FileOffset offset, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(batt::make_printable(buffer)) << BATT_INSPECT(offset)
                 << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, buffer, offset, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->write(req, ino, buffer, offset, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<usize>{push_status});
    }
  }

  /** \brief
   */ // 18/44
  template <typename Handler>
  void async_flush(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->flush(req, ino, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 19/44
  template <typename Handler>
  void async_release(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->release(req, ino, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 20/44
  template <typename Handler>
  void async_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi,
                   Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(datasync) << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, datasync, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->fsync(req, ino, datasync, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 21/44
  template <typename Handler>
  void async_opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->opendir(req, ino, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_file_info*>{push_status});
    }
  }

  /** \brief
   */ // 22/44
  template <typename Handler>
  void async_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, DirentOffset off,
                     fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION                       //
                 << BATT_INSPECT(req)                        //
                 << BATT_INSPECT(ino) << BATT_INSPECT(size)  //
                 << BATT_INSPECT(off)                        //
                 << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, size, off, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->readdir(req, ino, size, off, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::FuseReadDirData>{push_status});
    }
  }

  /** \brief
   */ // 23/44
  template <typename Handler>
  void async_releasedir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino)   //
                 << BATT_INSPECT(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->releasedir(req, ino, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 24/44
  template <typename Handler>
  void async_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi,
                      Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION      //
                 << BATT_INSPECT(req)       //
                 << BATT_INSPECT(ino)       //
                 << BATT_INSPECT(datasync)  //
                 << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, datasync, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->fsyncdir(req, ino, datasync, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 25/44
  template <typename Handler>
  void async_statfs(fuse_req_t req, fuse_ino_t ino, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->statfs(req, ino));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const struct statvfs*>{push_status});
    }
  }

  /** \brief
   */ // 26/44
  template <typename Handler>
  void async_set_extended_attribute(fuse_req_t req, fuse_ino_t ino,
                                    const FuseImplBase::ExtendedAttribute& attr, int flags,
                                    Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION                        //
                 << BATT_INSPECT(req)                         //
                 << BATT_INSPECT(ino)                         //
                 << BATT_INSPECT(batt::make_printable(attr))  //
                 << BATT_INSPECT(flags);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, attr, flags, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->set_extended_attribute(req, ino, attr, flags));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 27/44
  template <typename Handler>
  void async_get_extended_attribute(fuse_req_t req, fuse_ino_t ino, const char* name, size_t size,
                                    Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino)   //
                 << BATT_INSPECT(name)  //
                 << BATT_INSPECT(size);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, name, size, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->get_extended_attribute(req, ino, name, size));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)
      (batt::StatusOr<FuseImplBase::FuseGetExtendedAttributeReply>{push_status});
    }
  }

  /** \brief
   */ // 29/44
  template <typename Handler>
  void async_remove_extended_attribute(fuse_req_t req, fuse_ino_t ino, const char* name,
                                       Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino)   //
                 << BATT_INSPECT(name);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, name, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->remove_extended_attribute(req, ino, name));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 30/44
  template <typename Handler>
  void async_check_access(fuse_req_t req, fuse_ino_t ino, int mask, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino)   //
                 << BATT_INSPECT(mask);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, mask, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->check_access(req, ino, mask));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 31/44
  template <typename Handler>
  void async_create(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
                    fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION                //
                 << BATT_INSPECT(req)                 //
                 << BATT_INSPECT(parent)              //
                 << BATT_INSPECT(name)                //
                 << BATT_INSPECT(DumpFileMode{mode})  //
                 << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, parent, name, mode, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->create(req, parent, name, mode, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)
      (batt::StatusOr<FuseImplBase::FuseCreateReply>{push_status});
    }
  }

  /** \brief
   */ // 38/44
  template <typename Handler>
  void async_retrieve_reply(fuse_req_t req, void* cookie, fuse_ino_t ino, off_t offset,
                            struct fuse_bufvec* bufv, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION    //
                 << BATT_INSPECT(req)     //
                 << BATT_INSPECT(cookie)  //
                 << BATT_INSPECT(ino)     //
                 << BATT_INSPECT(offset)  //
                 << BATT_INSPECT(bufv);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, cookie, ino, offset, bufv, handler = BATT_FORWARD(handler)] {
          auto on_scope_exit = batt::finally([&] {
            BATT_FORWARD(handler)();
          });
          this->derived_this()->retrieve_reply(req, cookie, ino, offset, bufv);
        });

    if (!push_status.ok()) {
      LLFS_LOG_WARNING() << "Could not push request to work queue;" << BATT_INSPECT(push_status);
      BATT_FORWARD(handler)();
    }
  }

  /** \brief
   */ // 39/44
  template <typename Handler>
  void async_forget_multiple_inodes(fuse_req_t req, batt::Slice<fuse_forget_data> forgets,
                                    Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
        /*<< BATT_INSPECT(batt::make_printable(forgets)) TODO [tastolfi 2023-06-30] */;

    batt::Status push_status =
        this->work_queue_->push_job([this, req, forgets, handler = BATT_FORWARD(handler)] {
          auto on_scope_exit = batt::finally([&] {
            BATT_FORWARD(handler)();
          });
          this->derived_this()->forget_multiple_inodes(req, forgets);
        });

    if (!push_status.ok()) {
      LLFS_LOG_WARNING() << "Could not push request to work queue;" << BATT_INSPECT(push_status);
      BATT_FORWARD(handler)();
    }
  }

  /** \brief
   */ // 41/44
  template <typename Handler>
  void async_file_allocate(fuse_req_t req, fuse_ino_t ino, int mode, FileOffset offset,
                           FileLength length, fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION                //
                 << BATT_INSPECT(req)                 //
                 << BATT_INSPECT(ino)                 //
                 << BATT_INSPECT(DumpFileMode{mode})  //
                 << BATT_INSPECT(offset)              //
                 << BATT_INSPECT(length)              //
                 << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, mode, offset, length, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->file_allocate(req, ino, mode, offset, length, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 42/44
  template <typename Handler>
  void async_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, DirentOffset offset,
                         fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION    //
                 << BATT_INSPECT(req)     //
                 << BATT_INSPECT(ino)     //
                 << BATT_INSPECT(size)    //
                 << BATT_INSPECT(offset)  //
                 << BATT_INSPECT(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, size, offset, fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->readdirplus(req, ino, size, offset, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::FuseReadDirData>{push_status});
    }
  }

 private:
  std::shared_ptr<WorkQueue> work_queue_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class FuseSession
{
 public:
  template <typename Impl, typename... ImplArgs>
  static batt::StatusOr<FuseSession> from_args(int argc, char* argv[],
                                               batt::StaticType<Impl> /*impl*/,
                                               ImplArgs&&... impl_args)
  {
    FuseSession instance{argc, argv};

    if (fuse_parse_cmdline(&instance.args_, &instance.opts_) != 0) {
      return {batt::StatusCode::kInvalidArgument};
    }

    instance.impl_ = std::make_unique<Impl>(BATT_FORWARD(impl_args)...);

    instance.session_.reset(fuse_session_new(&instance.args_, Impl::get_fuse_lowlevel_ops(),
                                             sizeof(struct fuse_lowlevel_ops),
                                             instance.impl_.get()));

    if (instance.session_ == nullptr) {
      LLFS_LOG_ERROR() << "fuse_sesion_new returned NULL";
      return {batt::StatusCode::kInternal};
    }

    {
      const int retval = fuse_set_signal_handlers(instance.session_.get());
      if (retval != 0) {
        LLFS_LOG_ERROR() << "fuse_set_signal_handlers returned " << retval;
        return {batt::StatusCode::kInternal};
      }
    }
    {
      const int retval = fuse_session_mount(instance.session_.get(), instance.opts_.mountpoint);
      if (retval != 0) {
        LLFS_LOG_ERROR() << "fuse_session_mount returned " << retval;
        return {batt::StatusCode::kInternal};
      }
      instance.mounted_ = true;
    }

    return instance;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  FuseSession(const FuseSession&) = delete;
  FuseSession& operator=(const FuseSession&) = delete;

  FuseSession(FuseSession&&) = default;
  FuseSession& operator=(FuseSession&&) = default;

  ~FuseSession() noexcept
  {
    if (this->session_ != nullptr) {
      if (this->mounted_) {
        fuse_session_unmount(this->session_.get());
        this->mounted_ = false;
      }
      fuse_session_destroy(this->session_.release());
    }
  }

  int run() noexcept
  {
    if (this->opts_.singlethread) {
      return fuse_session_loop(this->session_.get());
    }

    struct fuse_loop_config config;
    std::memset(&config, 0, sizeof(config));

    config.clone_fd = this->opts_.clone_fd;
    config.max_idle_threads = this->opts_.max_idle_threads;

    return fuse_session_loop_mt(this->session_.get(), &config);
  }

 private:
  FuseSession(int argc, char* argv[]) noexcept : args_ FUSE_ARGS_INIT(argc, argv)
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct fuse_args args_;

  struct fuse_cmdline_opts opts_;

  batt::UniqueNonOwningPtr<fuse_session> session_;

  std::unique_ptr<FuseImplBase> impl_;

  bool mounted_ = false;
};

}  //namespace llfs

#endif  // LLFS_FUSE_HPP

#include <llfs/fuse.ipp>
