//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/fuse.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto FuseImplBase::const_buffer_vec_from_bufv(const fuse_bufvec& bufv)
    -> batt::StatusOr<ConstBufferVec>
{
  if (bufv.idx > bufv.count) {
    return {batt::status_from_errno(EINVAL)};
  }

  ConstBufferVec vec;

  usize offset = bufv.off;
  for (usize i = bufv.idx; i < bufv.count; ++i, offset = 0) {
    const fuse_buf& buf = bufv.buf[i];

    if (buf.flags & (FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK | FUSE_BUF_FD_RETRY)) {
      return {batt::Status{batt::StatusCode::kUnimplemented}};
    }

    vec.emplace_back(batt::ConstBuffer{buf.mem, buf.size} + offset);
  }

  return {std::move(vec)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ int FuseImplBase::errno_from_status(batt::Status status)
{
  if (status.ok()) {
    return 0;
  }

  static const batt::Status::CodeGroup* errno_group = &(batt::status_from_errno(EIO).group());

  if (&(status.group()) == errno_group) {
    int e = status.code_index_within_group();
    LLFS_VLOG(2) << "(status => errno) " << e << " " << std::strerror(e);
    return e;
  }

  if (status == batt::StatusCode::kUnimplemented) {
    return ENOTSUP;
  }

  return EINVAL;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int FuseImplBase::invoke_fuse_reply_buf(fuse_req_t req, const batt::ConstBuffer& cb)
{
  return fuse_reply_buf(req, static_cast<const char*>(cb.data()), cb.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int FuseImplBase::invoke_fuse_reply_iov(fuse_req_t req, const batt::Slice<batt::ConstBuffer>& cbs)
{
  if (FuseImplBase::can_cast_iovec_to_const_buffer()) {
    return fuse_reply_iov(req, reinterpret_cast<const struct iovec*>(cbs.begin()), cbs.size());
  }

  batt::SmallVec<iovec, 8> tmp;
  for (const batt::ConstBuffer& cb : cbs) {
    tmp.emplace_back(iovec{const_cast<void*>(cb.data()), cb.size()});
  }
  return fuse_reply_iov(req, tmp.data(), tmp.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int FuseImplBase::invoke_fuse_reply_data(fuse_req_t req, const FuseConstBufferVec& v)
{
  batt::SmallVec<char, kDirectIOBlockSize> tmp_storage;

  tmp_storage.resize(sizeof(fuse_bufvec) - sizeof(fuse_buf) +
                     sizeof(fuse_buf) * std::min<usize>(1, v.buffers.size()));

  std::memset(tmp_storage.data(), 0, tmp_storage.size());

  fuse_bufvec* const fbv = reinterpret_cast<fuse_bufvec*>(tmp_storage.data());

  fbv->count = v.buffers.size();
  fbv->idx = v.current_buffer_index;
  fbv->off = v.current_buffer_offset;

  {
    usize i = 0;
    for (const FuseConstBuffer& fcb : v.buffers) {
      fuse_buf& fb = fbv->buf[i];

      batt::case_of(  //
          fcb,        //

          //----- --- -- -  -  -   -
          [&fb](const batt::ConstBuffer& cb) {
            fb.size = cb.size();
            fb.mem = const_cast<void*>(cb.data());
          },

          //----- --- -- -  -  -   -
          [&fb](const OwnedConstBuffer& ocb) {
            fb.size = ocb.buffer.size();
            fb.mem = const_cast<void*>(ocb.buffer.data());
          },

          //----- --- -- -  -  -   -
          [&fb](const FileDataRef& fdr) {
            fb.size = fdr.size;
            fb.flags = (fuse_buf_flags)((int)fb.flags | (int)FUSE_BUF_IS_FD);
            fb.fd = fdr.fd.value();
            if (fdr.offset) {
              fb.flags = (fuse_buf_flags)((int)fb.flags | (int)FUSE_BUF_FD_SEEK);
              fb.pos = *fdr.offset;
            }
            if (fdr.should_retry) {
              fb.flags = (fuse_buf_flags)((int)fb.flags | (int)FUSE_BUF_FD_RETRY);
            }
          });

      ++i;
    }
  }

  return fuse_reply_data(req, fbv, /*flags=*/(fuse_buf_copy_flags)0);
  //                                                      ^
  // TODO [tastolfi 2023-06-28] support fuse_buf_copy_flags
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const fuse_file_info& t)
{
  return out << "fuse_file_info{"                       //
             << ".flags=" << t.flags                    //
             << ", .write_page=" << t.writepage         //
             << ", .direct_io=" << t.direct_io          //
             << ", .keep_cache=" << t.keep_cache        //
             << ", .flush=" << t.flush                  //
             << ", .nonseekable=" << t.nonseekable      //
             << ", .flock_release=" << t.flock_release  //
             << ", .cache_readdir=" << t.cache_readdir  //
             << ", .fh=" << t.fh                        //
             << ", .lock_owner=" << t.lock_owner        //
             << ", .poll_events=" << t.poll_events      //
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const fuse_file_info* t)
{
  if (!t) {
    return out << (void*)t;
  }
  return out << (void*)t << ":" << *t;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const DumpStat& t)
{
  return out << "stat{"                                       //
             << ".st_dev=" << t.s.st_dev                      //
             << ", .st_ino=" << t.s.st_ino                    //
             << ", .st_mode=" << std::bitset<9>{t.s.st_mode}  //
             << ", .st_size=" << t.s.st_size                  //
             << ", .st_nlink=" << t.s.st_nlink                //
             << ", .st_uid=" << t.s.st_uid                    //
             << ", .st_gid=" << t.s.st_gid                    //
             << ", .st_rdev=" << t.s.st_rdev                  //
             << ", .st_blksize=" << t.s.st_blksize            //
             << ", .st_blocks=" << t.s.st_blocks              //
             << ", ,}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const DumpFileMode t)
{
  if (S_ISBLK(t.mode)) {
    out << 'b';
  } else if (S_ISCHR(t.mode)) {
    out << 'c';
  } else if (S_ISDIR(t.mode)) {
    out << 'd';
  } else if (S_ISFIFO(t.mode)) {
    out << 'p';
  } else if (S_ISLNK(t.mode)) {
    out << 'l';
  } else {
    out << '-';
  }
  if ((t.mode & S_IRUSR) != 0) {
    out << 'r';
  } else {
    out << '-';
  }
  if ((t.mode & S_IWUSR) != 0) {
    out << 'w';
  } else {
    out << '-';
  }
  if ((t.mode & S_IXUSR) != 0) {
    out << 'x';
  } else {
    out << '-';
  }
  if ((t.mode & S_IRGRP) != 0) {
    out << 'r';
  } else {
    out << '-';
  }
  if ((t.mode & S_IWGRP) != 0) {
    out << 'w';
  } else {
    out << '-';
  }
  if ((t.mode & S_IXGRP) != 0) {
    out << 'x';
  } else {
    out << '-';
  }
  if ((t.mode & S_IROTH) != 0) {
    out << 'r';
  } else {
    out << '-';
  }
  if ((t.mode & S_IWOTH) != 0) {
    out << 'w';
  } else {
    out << '-';
  }
  if ((t.mode & S_IXOTH) != 0) {
    out << 'x';
  } else {
    out << '-';
  }
  return out;
}

}  //namespace llfs
