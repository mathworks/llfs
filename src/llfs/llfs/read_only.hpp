#pragma once
#ifndef LLFS_READ_ONLY_HPP
#define LLFS_READ_ONLY_HPP

namespace llfs {

enum struct ReadOnly : bool {
  kFalse = false,
  kTrue = true,
};

}  // namespace llfs

#endif  // LLFS_READ_ONLY_HPP
