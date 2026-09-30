//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STABLE_STRING_STORE_HPP
#define LLFS_STABLE_STRING_STORE_HPP

#include <llfs/buffer.hpp>
#include <llfs/data_packer.hpp>
#include <llfs/int_types.hpp>

#include <batteries/algo/parallel_copy.hpp>
#include <batteries/assert.hpp>
#include <batteries/math.hpp>

#include <batteries/async/worker_pool.hpp>
#include <batteries/buffer.hpp>

#include <array>
#include <memory>
#include <string_view>
#include <type_traits>
#include <vector>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A class that allows the user to efficiently allocate and copy string data in memory that
 * is scoped to the lifetime of the object itself.
 */
template <usize kStaticSize, usize kDynamicSize>
class BasicStableStringStore
{
 public:
  using StorageUnit = std::aligned_storage_t<64, 64>;

  static constexpr usize kUnitSizeLog2 = batt::log2_ceil(sizeof(StorageUnit));

  static constexpr usize kStaticSizeLog2 = std::max<usize>(kUnitSizeLog2,  //
                                                           batt::log2_ceil(kStaticSize));
  static constexpr usize kDynamicSizeLog2 = std::max<usize>(kUnitSizeLog2,  //
                                                            batt::log2_ceil(kDynamicSize));

  static constexpr usize kStaticAllocSize = usize{1} << kStaticSizeLog2;
  static constexpr usize kDynamicAllocSize = usize{1} << kDynamicSizeLog2;

  static constexpr usize kStaticAllocUnitsLog2 = (kStaticSizeLog2 - kUnitSizeLog2);
  static constexpr usize kDynamicAllocUnitsLog2 = (kDynamicSizeLog2 - kUnitSizeLog2);

  static constexpr usize kStaticAllocUnits = usize{1} << kStaticAllocUnitsLog2;
  static constexpr usize kDynamicAllocUnits = usize{1} << kDynamicAllocUnitsLog2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BasicStableStringStore();

  BasicStableStringStore(const BasicStableStringStore&) = delete;
  BasicStableStringStore& operator=(const BasicStableStringStore&) = delete;

  /** \brief Allocates a buffer of size `n` bytes.
   */
  MutableBuffer allocate(usize n);

  /** \brief Copies the given `string_view` into a memory location managed by this
   * `BasicStableStringStore` instance, and returns a `string_view` pointing to the stored data. The
   * `worker_pool`, if provided, is used the parallelize the copying process if necessary.
   */
  std::string_view store(const std::string_view& s,
                         batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool());

  /** \brief Copies the given `ConstBuffer` into a memory location managed by this
   * `BasicStableStringStore` instance as string data, and returns a `ConstBuffer` pointing to the
   * stored data.
   */
  ConstBuffer store(const ConstBuffer& buffer,
                    batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool())
  {
    const std::string_view s = this->store(
        std::string_view{static_cast<const char*>(buffer.data()), buffer.size()}, worker_pool);

    return ConstBuffer{s.data(), s.size()};
  }

  /** \brief Concatenates multiple chunks of data and copies the concatenation into a contiguous
   * buffer of memory.
   */
  template <typename... Parts>
  ConstBuffer concat(Parts&&... parts)
  {
    usize total_size = 0;

    // Compute the total amount of memory needed to be allocated for the result of the
    // concatenation.
    //
    const auto add_to_total = [&total_size](auto&& part) {
      total_size += batt::as_const_buffer(part).size();
      return 0;
    };

    (add_to_total(parts), ...);

    MutableBuffer mbuf = this->allocate(total_size);
    MutableBuffer cbuf = mbuf;

    // Copy each part to memory.
    //
    const auto copy_part = [&mbuf](auto&& part) {
      auto src = batt::as_const_buffer(part);
      std::memcpy(mbuf.data(), src.data(), src.size());
      mbuf += src.size();
      return 0;
    };

    (copy_part(parts), ...);

    return cbuf;
  }

 private:
  /** \brief The statically allocated block of memory that is initialized when this
   * `BasicStableStringStore` instance is created, used as a starting point for memory allocations
   * done by this instance.
   */
  std::array<StorageUnit, kStaticAllocUnits> chunk0_;

  /** \brief A collection of dynamically allocated memory blocks, managing the chunks allocated
   * beyond `chunk0_`.
   */
  std::vector<std::unique_ptr<StorageUnit[]>> chunks_;

  /** \brief A buffer representing the current chunk of memory that has free space available for
   * allocation.
   */
  MutableBuffer free_chunk_;
};

using StableStringStore = BasicStableStringStore<64, 4096>;

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kStaticSize, usize kDynamicSize>
BasicStableStringStore<kStaticSize, kDynamicSize>::BasicStableStringStore()
    : free_chunk_{std::addressof(this->chunk0_), sizeof(this->chunk0_)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kStaticSize, usize kDynamicSize>
MutableBuffer BasicStableStringStore<kStaticSize, kDynamicSize>::allocate(usize n)
{
  // Check if the current free_chunk_ is large enough to hold n bytes. If it isn't, we need to
  // dynamically allocate a new chunk.
  //
  if (this->free_chunk_.size() < n) {
    // Allocate new chunk, add it to the list of dynamically allocated chunks, and point free_chunk_
    // to this new chunk.
    //
    const usize new_chunk_size = batt::round_up_bits(kDynamicSizeLog2, n);
    const usize n_units = new_chunk_size >> kUnitSizeLog2;
    std::unique_ptr<StorageUnit[]> new_chunk{new StorageUnit[n_units]};
    char* const new_chunk_data = (char*)new_chunk.get();
    this->chunks_.emplace_back(std::move(new_chunk));
    this->free_chunk_ = MutableBuffer{new_chunk_data, new_chunk_size};
  }

  BATT_CHECK_GE(this->free_chunk_.size(), n);

  // Return the newly allocated chunk and advance the start of the free_chunk_ buffer by n bytes to
  // indicate that this region of memory is now occupied.
  //
  MutableBuffer stable_buffer{this->free_chunk_.data(), n};
  this->free_chunk_ += n;
  return stable_buffer;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kStaticSize, usize kDynamicSize>
std::string_view BasicStableStringStore<kStaticSize, kDynamicSize>::store(
    const std::string_view& s, batt::WorkerPool& worker_pool)
{
  // Allocate a buffer the size of the input string data.
  //
  MutableBuffer stable_buffer = this->allocate(s.size());

  BATT_CHECK_EQ(stable_buffer.size(), s.size());

  // Check if we would benefit from parallelizing the copying process. If we do have workers in the
  // worker_pool and the size of the string data isn't too small, parallelize.
  //
  if (worker_pool.size() == 0 || s.size() < llfs::DataPacker::min_parallel_copy_size()) {
    std::memcpy(stable_buffer.data(), s.data(), s.size());
  } else {
    batt::ScopedWorkContext work_context{worker_pool};

    const batt::TaskCount max_tasks{worker_pool.size() + 1};
    const batt::TaskSize min_task_size{llfs::DataPacker::min_parallel_copy_size()};

    const char* const src_begin = s.data();
    const char* const src_end = src_begin + s.size();
    char* const dst_begin = static_cast<char*>(stable_buffer.data());

    batt::parallel_copy(work_context, src_begin, src_end, dst_begin, min_task_size, max_tasks);
  }

  // Return the copy.
  //
  return std::string_view{static_cast<const char*>(stable_buffer.data()), stable_buffer.size()};
}

}  // namespace llfs

#endif  // LLFS_STABLE_STRING_STORE_HPP
