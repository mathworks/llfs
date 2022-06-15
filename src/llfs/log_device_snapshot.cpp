//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/log_device_snapshot.hpp>
//

#include <boost/functional/hash.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize LogDeviceSnapshot::compute_hash_value() const
{
  usize seed = 0;

  boost::hash_combine(seed, this->trim_pos());
  boost::hash_combine(seed, this->flush_pos());
  boost::hash_combine(seed, this->commit_pos());
  boost::hash_range(seed, this->begin(), this->end());

  return seed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator==(const LogDeviceSnapshot& l, const LogDeviceSnapshot& r)
{
  return l.trim_pos() == r.trim_pos()         //
         && l.flush_pos() == r.flush_pos()    //
         && l.commit_pos() == r.commit_pos()  //
         && (l.bytes() == r.bytes()           //
             || (l.size() == r.size() &&      //
                 (0 == std::memcmp(l.bytes(), r.bytes(), l.size()))));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize hash_value(const LogDeviceSnapshot& s)
{
  return s.hash_value_;
}

}  // namespace llfs
