#pragma once
#ifndef LLFS_POINTERS_HPP
#define LLFS_POINTERS_HPP

#include <turtle/util/pointers.hpp>

namespace llfs {

using ::turtle_db::NoopDeleter;
using ::turtle_db::UniqueNonOwningPtr;

}  // namespace llfs

#endif  // LLFS_POINTERS_HPP
