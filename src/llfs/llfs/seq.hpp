#pragma once
#ifndef LLFS_SEQ_HPP
#define LLFS_SEQ_HPP

#include <batteries/seq.hpp>
#include <batteries/seq/empty.hpp>

namespace llfs {

namespace seq = ::batt::seq;
using ::batt::as_seq;
using ::batt::BoxedSeq;
using ::batt::SeqItem;

}  // namespace llfs

#endif  // LLFS_SEQ_HPP
