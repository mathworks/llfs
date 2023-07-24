//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_buffer_pool.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingBufferPool::IoRingBufferPool(IoRing& io_ring, BufferCount count,
                                                BufferSize size) noexcept
    : io_ring_{io_ring}
{
  (void)count;
  (void)size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingBufferPool::AllocatedBuffer::~AllocatedBuffer() noexcept
{
  this->pool_.free_buffer(this->index_, this->buffer_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingBufferPool::free_buffer(int index, const batt::MutableBuffer& buffer)
{
  const usize prior_count = this->free_count_->fetch_add(1);
  const usize observed_count = prior_count + 1;
  const usize shard_i = observed_count % Self::kShardCount;

  (void)prior_count;
  (void)observed_count;
  (void)shard_i;
  (void)buffer;
  (void)index;
}

}  //namespace llfs
