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
#include <llfs/int_types.hpp>

#include <batteries/async/worker_pool.hpp>
#include <batteries/buffer.hpp>

#include <array>
#include <memory>
#include <string_view>
#include <vector>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A class that allows the user to efficiently allocate and copy string data in memory that
 * is scoped to the lifetime of the object itself.
 */
class StableStringStore
{
 public:
  static constexpr usize kStaticAllocSize = 32;
  static constexpr usize kDynamicAllocSize = 4096;

  StableStringStore();

  StableStringStore(const StableStringStore&) = delete;
  StableStringStore& operator=(const StableStringStore&) = delete;

  /** \brief Allocates a buffer of size `n` bytes.
   */
  MutableBuffer allocate(usize n);

  /** \brief Copies the given `string_view` into a memory location managed by this
   * `StableStringStore` instance, and returns a `string_view` pointing to the stored data. The
   * `worker_pool`, if provided, is used the parallelize the copying process if necessary.
   */
  std::string_view store(const std::string_view& s,
                         batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool());

  /** \brief Copies the given `ConstBuffer` into a memory location managed by this
   * `StableStringStore` instance as string data, and returns a `ConstBuffer` pointing to the stored
   * data.
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
   * `StableStringStore` instance is created, used as a starting point for memory allocations done
   * by this instance.
   */
  std::array<char, kStaticAllocSize> chunk0_;

  /** \brief A collection of dynamically allocated memory blocks, managing the chunks allocated
   * beyond `chunk0_`.
   */
  std::vector<std::unique_ptr<char[]>> chunks_;

  /** \brief A buffer representing the current chunk of memory that has free space available for
   * allocation.
   */
  MutableBuffer free_chunk_;
};

}  // namespace llfs

#endif  // LLFS_STABLE_STRING_STORE_HPP