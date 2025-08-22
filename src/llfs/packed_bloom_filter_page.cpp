//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_bloom_filter_page.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ PageLayoutId PackedBloomFilterPage::page_layout_id()
{
  static const PageLayoutId id = PageLayoutId::from_str("bloomflt");
  return id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PackedBloomFilterPage::check_magic() const
{
  BATT_CHECK_EQ(this->magic, PackedBloomFilterPage::kMagic);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PackedBloomFilterPage::require_magic() const
{
  if (this->magic != PackedBloomFilterPage::kMagic) {
    return batt::StatusCode::kDataLoss;
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 PackedBloomFilterPage::compute_xxh3_checksum() const
{
  const void* const start = std::addressof(this->magic);
  const void* const end = this->bloom_filter.filter_data_end();

  return XXH3_64bits(start, byte_distance(start, end));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 PackedBloomFilterPage::compute_bit_count() const
{
  u64 count = 0;
  for (u64 word : this->bloom_filter.get_words()) {
    count += batt::bit_count(word);
  }
  return count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PackedBloomFilterPage::check_integrity() const
{
  this->check_magic();
  if (this->bit_count != 0) {
    BATT_CHECK_EQ(this->bit_count, this->compute_bit_count());
  }
  if (this->xxh3_checksum != 0) {
    BATT_CHECK_EQ(this->xxh3_checksum, this->compute_xxh3_checksum());
  }
}

}  //namespace llfs
