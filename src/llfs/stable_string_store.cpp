//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/stable_string_store.hpp>
//

#include <llfs/data_packer.hpp>

#include <batteries/algo/parallel_copy.hpp>
#include <batteries/assert.hpp>
#include <batteries/math.hpp>

namespace llfs {
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StableStringStore::StableStringStore() : free_chunk_{this->chunk0_.data(), this->chunk0_.size()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer StableStringStore::allocate(usize n)
{
  // Check if the current free_chunk_ is large enough to hold n bytes. If it isn't, we need to
  // dynamically allocate a new chunk.
  //
  if (this->free_chunk_.size() < n) {
    // Allocate new chunk, add it to the list of dynamically allocated chunks, and point free_chunk_
    // to this new chunk.
    //
    const usize new_chunk_size = batt::round_up_bits(batt::log2_ceil(kDynamicAllocSize), n);
    std::unique_ptr<char[]> new_chunk{new char[new_chunk_size]};
    char* const new_chunk_data = new_chunk.get();
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
std::string_view StableStringStore::store(const std::string_view& s, batt::WorkerPool& worker_pool)
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