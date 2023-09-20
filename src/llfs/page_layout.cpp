//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_layout.hpp>
//

#include <llfs/crc.hpp>

#include <boost/uuid/uuid_io.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 compute_page_crc64(const PageBuffer& page)
{
  BATT_PANIC() << "this is supposed to be disabled right now!";

  PackedPageHeader header = get_page_header(page);

  header.crc32 = 0;

  auto crc64 = make_crc64();

  const u8* page_bytes = reinterpret_cast<const u8*>(&page);

  crc64.process_bytes(&header, sizeof(PackedPageHeader));

  crc64.process_bytes(page_bytes + sizeof(PackedPageHeader),
                      header.unused_begin.value() - sizeof(PackedPageHeader));

  crc64.process_bytes(page_bytes + header.unused_end.value(),
                      page.size() - header.unused_end.value());

  return crc64.checksum();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status finalize_page_header(PageBuffer* page, const Interval<u64>& unused_region)
{
  PackedPageHeader* header = mutable_page_header(page);

  header->magic = PackedPageHeader::kMagic;
  header->unused_begin = unused_region.lower_bound;
  header->unused_end = unused_region.upper_bound;

  BATT_CHECK_EQ(header->unused_begin.value(), unused_region.lower_bound);
  BATT_CHECK_EQ(header->unused_end.value(), unused_region.upper_bound);

#if !LLFS_DISABLE_PAGE_CRC
  // TODO [tastolfi 2021-09-07] Decide and clean up: 32 vs 64 bits
  header->crc32 = compute_page_crc64(*page);
#endif

  return OkStatus();
}

std::ostream& operator<<(std::ostream& out, const PackedPageUserSlot& t)
{
  return out << "{.user_id=" << t.user_id << ", .slot_offset=" << t.slot_offset.value() << ",}";
}

std::ostream& operator<<(std::ostream& out, const PackedPageHeader& t)
{
  return out << "PackedPageHeader{.magic=" << std::hex << t.magic.value()
             << ", .page_id=" << t.page_id.unpack() << ", .layout_id=" << t.layout_id
             << ", .crc32=" << t.crc32.value() << ", .unused_begin=" << std::dec
             << t.unused_begin.value() << ", .unused_end=" << t.unused_end.value()
             << ", .size=" << t.size << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> trace_refs(const PackedArray<PackedPageId>& packed)
{
  return as_seq(packed) | seq::map([](const PackedPageId& ppi) {
           return ppi.unpack();
         }) |
         seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageId> trace_refs(const BoxedSeq<PageId>& page_ids)
{
  return batt::make_copy(page_ids);
}

}  // namespace llfs
