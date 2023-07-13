//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEM_FILE_HANDLE_HPP
#define LLFS_MEM_FILE_HANDLE_HPP

#include <llfs/mem_inode.hpp>

#include <llfs/api_types.hpp>

#include <batteries/shared_ptr.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class MemFileHandle : batt::RefCounted<MemFileHandle>
{
 public:
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

}  //namespace llfs

#endif  // LLFS_MEM_FILE_HANDLE_HPP
