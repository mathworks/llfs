//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_SCENARIO_RUNNER_HPP
#define LLFS_TESTING_SCENARIO_RUNNER_HPP

#include <llfs/config.hpp>
//

#include <llfs/int_types.hpp>

#include <batteries/cpu_align.hpp>
#include <batteries/utility.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

namespace llfs {
namespace testing {

/** \brief Runs different test scenarios, each defined by an RNG seed value, on different threads.
 *
 * Example usage:
 *
 * ```
 * struct MyTestScenario {
 *   std::default_random_engine rng;
 *
 *   explicit MyTestScenario(llfs::RandomSeed seed) noexcept : rng{seed}
 *   {
 *   }
 *
 *   void run() noexcept
 *   {
 *      // do test stuff here, probably using `this->rng`.
 *   }
 * };
 *
 * llfs::ScenarioRunner{}
 *   .n_seeds(1000)
 *   .n_threads(16)
 *   .n_updates(5)
 *   .run(batt::StaticType<MyTestScenario>{});
 * ```
 */
class ScenarioRunner
{
 public:
  using Self = ScenarioRunner;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new ScenarioRunner.
   */
  ScenarioRunner() noexcept
  {
  }

  /** \brief Sets the number of unique random seeds for this scenario runner.
   *
   * The default is 0, so tests should normally always call this function.
   *
   * \return *this
   */
  Self& n_seeds(u32 n) noexcept
  {
    this->n_seeds_ = n;
    return *this;
  }

  /** \brief Sets the number of threads to use when running scenarios in parallel.
   *
   * Defaults to `std::thread::hardware_concurrency()`, the number of hardware threads available.
   *
   * \return *this
   */
  Self& n_threads(usize n) noexcept
  {
    this->n_threads_ = n;
    return *this;
  }

  /** \brief Sets the number of update log (info) messages to print while running scenarios.
   *
   * For example, if n = 4, then progress messages will be printed at (roughly) 25%, 50%, 75%, and
   * 100%.
   *
   * Defaults to 10.
   *
   * \return *this
   */
  Self& n_updates(usize n) noexcept
  {
    this->n_updates_ = n;
    return *this;
  }

  /** \brief Instantiates the specified scenario type (`ScenarioT`) for each `RandomSeed` value (up
   * to a total of `this->n_seeds unique seeds), on a newly created thread pool, giving progress
   * reports as specified by `n_updates`.
   *
   * Type requirements on `ScenarioT`:
   *
   *  - Must have a public constructor that takes at least one arg of type `RandomSeed`.  The
   *    constructor may take additional inputs; these must be passed to `ScenarioRunner::run` as
   *    `extra_args`
   *  - Must have a public destructor
   *  - Must have a public member function `void run()`
   */
  template <typename ScenarioT, typename... ExtraArgs>
  void run(batt::StaticType<ScenarioT>, ExtraArgs&&... extra_args)
  {
    ASSERT_GT(this->n_seeds_, 0);

    const usize kUpdateInterval = (this->n_updates_ != 0) ? (this->n_seeds_ / this->n_updates_) : 0;

    testing::TestConfig test_config;

    u32 next_seed = test_config.get_random_seed();
    u32 seeds_remaining = this->n_seeds_;

    std::vector<std::thread> threads;
    std::vector<batt::CpuCacheLineIsolated<std::atomic<u32>>> progress(this->n_threads_);
    for (auto& p : progress) {
      p->store(0);
    }

    for (usize i = 0; i < this->n_threads_; ++i) {
      const u32 this_thread_n_seeds = seeds_remaining / (this->n_threads_ - i);
      //----- --- -- -  -  -   -
      threads.emplace_back(
          [&, thread_i = i, start_seed = next_seed, end_seed = next_seed + this_thread_n_seeds] {
            for (u32 seed = start_seed; seed < end_seed; seed += 1) {
              if (kUpdateInterval != 0) {
                LLFS_LOG_INFO_EVERY_N(kUpdateInterval) << "progress=" << [&](std::ostream& out) {
                  u32 total_progress = 0;
                  for (auto& p : progress) {
                    total_progress += p->load();
                  }
                  out << total_progress * 100 / this->n_seeds_ << "% (" << total_progress << "/"
                      << this->n_seeds_ << ")";
                };
              }

              ScenarioT scenario{RandomSeed{seed}, BATT_FORWARD(extra_args)...};
              ASSERT_NO_FATAL_FAILURE(scenario.run());

              progress[thread_i]->fetch_add(1);
            }
          });
      //----- --- -- -  -  -   -
      next_seed += this_thread_n_seeds;
      seeds_remaining -= this_thread_n_seeds;
    }

    for (std::thread& t : threads) {
      t.join();
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  u32 n_seeds_ = 0;
  usize n_threads_ = std::thread::hardware_concurrency();
  usize n_updates_ = 10;
};

}  //namespace testing
}  //namespace llfs

#endif  // LLFS_TESTING_SCENARIO_RUNNER_HPP
