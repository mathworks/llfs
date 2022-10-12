#include <batteries/async/runtime.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/stream_util.hpp>

namespace {

class LlfsRuntimeTestEnv : public testing::Environment
{
 public:
  LlfsRuntimeTestEnv() noexcept
  {
  }

  ~LlfsRuntimeTestEnv() override
  {
  }

  // Override this to define how to set up the environment.
  void SetUp() override
  {
    batt::EscapedStringLiteral::max_show_length() = 32;
  }

  // Override this to define how to tear down the environment.
  void TearDown() override
  {
    batt::Runtime::instance().halt();
  }
};

testing::Environment* const runtime_env = testing::AddGlobalTestEnvironment(new LlfsRuntimeTestEnv);

}  // namespace
