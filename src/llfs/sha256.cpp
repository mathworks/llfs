//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/sha256.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::Optional<Sha256> Sha256::from_str(const std::string_view& s)
{
  Sha256 result;

  const char* next_ch = s.data();
  const char* last_ch = s.data() + s.size();

  for (u8& byte : result.bytes) {
    byte = 0;
    {
      if (next_ch == last_ch) {
        return batt::None;
      }
      const char ch = *next_ch;

      if ('0' <= ch && ch <= '9') {
        byte = (ch - '0') << 4;
      } else if ('A' <= ch && ch <= 'F') {
        byte = (ch - 'A' + 0xa) << 4;
      } else if ('a' <= ch && ch <= 'f') {
        byte = (ch - 'a' + 0xa) << 4;
      } else {
        return batt::None;
      }
    }
    ++next_ch;
    {
      if (next_ch == last_ch) {
        return batt::None;
      }

      const char ch = *next_ch;

      if ('0' <= ch && ch <= '9') {
        byte |= (ch - '0');
      } else if ('A' <= ch && ch <= 'F') {
        byte |= (ch - 'A' + 0xa);
      } else if ('a' <= ch && ch <= 'f') {
        byte |= (ch - 'a' + 0xa);
      } else {
        return batt::None;
      }
    }
    ++next_ch;
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const Sha256& t)
{
  for (u8 next_byte : t.bytes) {
    out << batt::to_string(std::hex, std::setw(2), std::setfill('0'), (int)next_byte);
  }
  return out;
}

}  //namespace llfs
