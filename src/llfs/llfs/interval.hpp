#pragma once
#ifndef LLFS_INTERVAL_HPP
#define LLFS_INTERVAL_HPP

#include <batteries/interval.hpp>

namespace llfs {

using batt::BasicInterval;
using batt::CInterval;
using batt::GreatestLowerBound;
using batt::IClosed;
using batt::IClosedOpen;
using batt::Interval;
using batt::interval_traits_compatible;
using batt::IntervalTraits;
using batt::LeastUpperBound;
using batt::make_interval;

}  // namespace llfs

#endif  // LLFS_INTERVAL_HPP
