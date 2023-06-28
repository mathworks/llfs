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
/*static*/ int FuseImplBase::errno_from_status(batt::Status status)
{
  if (status.ok()) {
    return 0;
  }

  static const batt::Status prototype = batt::status_from_errno(EIO);

  if (&(status.group()) == &(prototype.group())) {
    int e = status.code_index_within_group();
    LLFS_VLOG(1) << "(status => errno) " << e << " " << std::strerror(e);
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
  batt::SmallVec<char, 512> tmp_storage;

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

}  //namespace llfs
