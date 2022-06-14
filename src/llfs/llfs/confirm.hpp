#pragma once
#ifndef LLFS_CONFIRM_HPP
#define LLFS_CONFIRM_HPP

namespace llfs {

enum struct ConfirmThisWillEraseAllMyData : bool {
  kNo = false,
  kYes = true,
};

}  // namespace llfs

#endif  // LLFS_CONFIRM_HPP
