//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_DATA_PACKER_ARENA_HPP
#define LLFS_DATA_PACKER_ARENA_HPP

#include <llfs/config.hpp>
//
#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>
#include <llfs/interval.hpp>
#include <llfs/optional.hpp>

#include <batteries/pointers.hpp>

#include <boost/range/iterator_range.hpp>

namespace llfs {

class DataPacker;

/*! \brief A sub-region of the buffer used for allocation/reservation.
 *
 * DataPackerArena is movable but not copyable.
 */
class DataPackerArena
{
  friend class DataPacker;

 public:
  explicit DataPackerArena(DataPacker* packer, boost::iterator_range<u8*> avail) noexcept;

  //----- --- -- -  -  -   -
  DataPackerArena(const DataPackerArena&) = delete;
  DataPackerArena& operator=(const DataPackerArena&) = delete;

  DataPackerArena(DataPackerArena&&) = default;
  DataPackerArena& operator=(DataPackerArena&&) = default;
  //----- --- -- -  -  -   -

  /*! \brief The number of bytes available in this DataPackerArena for reservation/allocation.
   */
  usize space() const;

  /*! \brief The original size of this arena.
   */
  usize capacity() const;

  /*! \brief Returns true iff some prior reserve or allocate has failed
   */
  bool full() const;

  /*! \brief Returns the available region of this arena as offsets relative to the packer's
   * buffer.
   */
  Interval<isize> unused() const;

  /*! \brief Clears this object, detaching it from its buffer and packer.
   */
  void invalidate();

  /*! \brief Set the `full` flag to true, disabling future reservation/allocation.
   */
  void set_full();

  /*! \brief Split this arena by removing `size` bytes from the front of the available region.
   */
  Optional<DataPackerArena> reserve_front(usize size);

  /*! \brief Split this arena by removing `size` bytes from the back of the available region.
   */
  Optional<DataPackerArena> reserve_back(usize size);

  /*! \brief Allocate `size` bytes at the front of the available region.
   */
  Optional<MutableBuffer> allocate_front(usize size);

  /*! \brief Allocate `size` bytes at the back of the available region.
   */
  Optional<MutableBuffer> allocate_back(usize size);

  /*! \brief Pack `n` as a variable-length integer at the front of this arena.
   *
   * \return nullptr if there isn't enough space to pack n; otherwise return pointer to the first
   * byte of the packed var-int.
   */
  u8* pack_varint(u64 n);

 private:
  /*! \brief The DataPacker object with which this DataPackerArena is associated, for sanity
   * checking.
   */
  DataPacker* get_packer() const;

  boost::iterator_range<u8*> nocheck_alloc_front(isize size);

  boost::iterator_range<u8*> nocheck_alloc_back(isize size);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize capacity_;
  batt::UniqueNonOwningPtr<DataPacker> packer_;
  boost::iterator_range<u8*> avail_;
  bool full_ = false;
};

}  //namespace llfs

#endif  // LLFS_DATA_PACKER_ARENA_HPP
