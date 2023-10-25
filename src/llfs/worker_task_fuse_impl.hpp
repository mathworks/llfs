//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_WORKER_TASK_FUSE_IMPL_HPP
#define LLFS_WORKER_TASK_FUSE_IMPL_HPP

#include <llfs/config.hpp>
//
#include <llfs/fuse.hpp>
#include <llfs/logging.hpp>
#include <llfs/worker_task.hpp>

#include <memory>
#include <utility>

namespace llfs {

template <typename Derived>
class WorkerTaskFuseImpl : public FuseImpl<Derived>
{
 public:
  explicit WorkerTaskFuseImpl(std::shared_ptr<WorkQueue>&& work_queue) noexcept
      : work_queue_{std::move(work_queue)}
  {
  }

  // 1/44 (init)
  // 2/44 (destroy)
  //
  //  - Not included since they are already non-async.

  /** \brief
   */ // 3/44
  template <typename Handler>
  void async_lookup(fuse_req_t req, fuse_ino_t parent, const char* name, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(parent)
                 << BATT_INSPECT(name);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, parent, name = std::string{name}, handler = BATT_FORWARD(handler)] {
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
  void async_get_attributes(fuse_req_t req, fuse_ino_t ino, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->get_attributes(req, ino));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::Attributes>{push_status});
    }
  }

  /** \brief
   */ // 6/44
  template <typename Handler>
  void async_set_attributes(fuse_req_t req, fuse_ino_t ino, const struct stat* attr, int to_set,
                            fuse_file_info* fi, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION    //
                 << BATT_INSPECT(req)     //
                 << BATT_INSPECT(ino)     //
                 << BATT_INSPECT(attr)    //
                 << BATT_INSPECT(to_set)  //
                 << BATT_INSPECT(fi);

    /* If the setattr was invoked from the ftruncate() system call
     * under Linux kernel versions 2.6.15 or later, the fi->fh will
     * contain the value set by the open method or will be undefined
     * if the open method didn't set any value.  Otherwise (not
     * ftruncate call, or kernel version earlier than 2.6.15) the fi
     * parameter will be NULL.
     *
     *  (from libfuse/include/fuse_lowlevel.h)
     */
    auto fh = [fi]() -> batt::Optional<FuseFileHandle> {
      if (fi) {
        return FuseFileHandle{fi->fh};
      }
      return batt::None;
    }();

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, attr = *attr, to_set, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->set_attributes(req, ino, &attr, to_set, fh));
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
        [this, req, parent, name = std::string{name}, mode, rdev, handler = BATT_FORWARD(handler)] {
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
        [this, req, parent, name = std::string{name}, mode, handler = BATT_FORWARD(handler)] {
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

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, parent, name = std::string{name}, handler = BATT_FORWARD(handler)] {
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

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, parent, name = std::string{name}, handler = BATT_FORWARD(handler)] {
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

    batt::Status push_status =
        this->work_queue_->push_job([this, req, link = std::string{link}, parent,
                                     name = std::string{name}, handler = BATT_FORWARD(handler)] {
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
        [this, req, parent, name = std::string{name}, newparent, newname = std::string{newname},
         flags, handler = BATT_FORWARD(handler)] {
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

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, newparent, newname = std::string{newname},
                                     handler = BATT_FORWARD(handler)] {
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

    BATT_CHECK_NOT_NULLPTR(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi = *fi, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->open(req, ino, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<const fuse_file_info*>{push_status});
    }
  }

  /** \brief
   */ // 16/44
  template <typename Handler>
  void async_read(fuse_req_t req, fuse_ino_t ino, size_t size, FileOffset offset, FuseFileHandle fh,
                  Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(size) << BATT_INSPECT(offset) << BATT_INSPECT(fh);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, size, offset, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->read(req, ino, size, offset, fh));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::FuseReadData>{push_status});
    }
  }

  /** \brief
   */ // 17/44
  template <typename Handler>
  void async_write(fuse_req_t req, fuse_ino_t ino, const batt::ConstBuffer& buffer,
                   FileOffset offset, FuseFileHandle fh, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(batt::make_printable(buffer)) << BATT_INSPECT(offset)
                 << BATT_INSPECT(fh);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, buffer, offset, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->write(req, ino, buffer, offset, fh));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<usize>{push_status});
    }
  }

  /** \brief
   */ // 18/44
  template <typename Handler>
  void async_flush(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fh);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->flush(req, ino, fh));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 19/44
  template <typename Handler>
  void async_release(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh, FileOpenFlags flags,
                     Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(fh);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fh, flags, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->release(req, ino, fh, flags));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 20/44
  template <typename Handler>
  void async_fsync(fuse_req_t req, fuse_ino_t ino, IsDataSync datasync, FuseFileHandle fh,
                   Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION << BATT_INSPECT(req) << BATT_INSPECT(ino)
                 << BATT_INSPECT(datasync) << BATT_INSPECT(fh);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, datasync, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->fsync(req, ino, datasync, fh));
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

    BATT_CHECK_NOT_NULLPTR(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fi = *fi, handler = BATT_FORWARD(handler)] {
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
                     FuseFileHandle fh, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino)   //
                 << BATT_INSPECT(size)  //
                 << BATT_INSPECT(off)   //
                 << BATT_INSPECT(fh);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, size, off, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->readdir(req, ino, size, off, fh));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::FuseReadDirData>{push_status});
    }
  }

  /** \brief
   */ // 23/44
  template <typename Handler>
  void async_releasedir(fuse_req_t req, fuse_ino_t ino, FuseFileHandle fh, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION  //
                 << BATT_INSPECT(req)   //
                 << BATT_INSPECT(ino)   //
                 << BATT_INSPECT(fh);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, ino, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->releasedir(req, ino, fh));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(push_status);
    }
  }

  /** \brief
   */ // 24/44
  template <typename Handler>
  void async_fsyncdir(fuse_req_t req, fuse_ino_t ino, IsDataSync datasync, FuseFileHandle fh,
                      Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION      //
                 << BATT_INSPECT(req)       //
                 << BATT_INSPECT(ino)       //
                 << BATT_INSPECT(datasync)  //
                 << BATT_INSPECT(fh);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, datasync, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->fsyncdir(req, ino, datasync, fh));
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

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, name = std::string{name}, size, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->get_extended_attribute(req, ino, name, size));
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

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, name = std::string{name}, handler = BATT_FORWARD(handler)] {
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

    BATT_CHECK_NOT_NULLPTR(fi);

    batt::Status push_status =
        this->work_queue_->push_job([this, req, parent, name = std::string{name}, mode, fi = *fi,
                                     handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->create(req, parent, name, mode, fi));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)
      (batt::StatusOr<FuseImplBase::FuseCreateReply>{push_status});
    }
  }

  /** \brief
   */ // 35/44
  template <typename Handler>
  void async_ioctl(fuse_req_t req, fuse_ino_t ino, unsigned int cmd, void* arg,
                   struct fuse_file_info* fi, unsigned flags, const batt::ConstBuffer& in_buf,
                   size_t out_bufsz, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION           //
                 << BATT_INSPECT(req)            //
                 << BATT_INSPECT(ino)            //
                 << BATT_INSPECT(cmd)            //
                 << BATT_INSPECT(arg)            //
                 << BATT_INSPECT(fi)             //
                 << BATT_INSPECT(flags)          //
                 << BATT_INSPECT(in_buf.size())  //
                 << BATT_INSPECT(out_bufsz);

    BATT_CHECK_NOT_NULLPTR(fi);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, cmd, arg, fi, flags, in_buf, out_bufsz, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->ioctl(req, ino, cmd, arg, fi, flags, in_buf, out_bufsz));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::FuseIoctlReply>{push_status});
    }
  }

  /** \brief
   */ // 37/44
  template <typename Handler>
  void async_write_buf(fuse_req_t req, fuse_ino_t ino, const FuseImplBase::ConstBufferVec& bufv,
                       FileOffset offset, FuseFileHandle fh, Handler&& handler)
  {
    // TODO [tastolfi 2023-07-12]  print bufv
    //
    LLFS_VLOG(1) << BATT_THIS_FUNCTION    //
                 << BATT_INSPECT(req)     //
                 << BATT_INSPECT(ino)     //
                 << BATT_INSPECT(offset)  //
                 << BATT_INSPECT(fh);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, bufv, offset, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)(this->derived_this()->write_buf(req, ino, bufv, offset, fh));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<usize>{push_status});
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
                         FuseFileHandle fh, Handler&& handler)
  {
    LLFS_VLOG(1) << BATT_THIS_FUNCTION    //
                 << BATT_INSPECT(req)     //
                 << BATT_INSPECT(ino)     //
                 << BATT_INSPECT(size)    //
                 << BATT_INSPECT(offset)  //
                 << BATT_INSPECT(fh);

    batt::Status push_status = this->work_queue_->push_job(
        [this, req, ino, size, offset, fh, handler = BATT_FORWARD(handler)] {
          BATT_FORWARD(handler)
          (this->derived_this()->readdirplus(req, ino, size, offset, fh));
        });

    if (!push_status.ok()) {
      BATT_FORWARD(handler)(batt::StatusOr<FuseImplBase::FuseReadDirData>{push_status});
    }
  }

 private:
  std::shared_ptr<WorkQueue> work_queue_;
};

}  //namespace llfs

#endif  // LLFS_WORKER_TASK_FUSE_IMPL_HPP
