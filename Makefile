#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

.PHONY: clean build build-nodoc install create test publish docker-build docker-push docker unlink

CONAN_PROFILE := $(shell test -f /etc/conan_profile.default && echo '/etc/conan_profile.default' || echo 'default')
$(info CONAN_PROFILE is $(CONAN_PROFILE))

#----- --- -- -  -  -   -
# Force some requirements to build from source to workaround
# OS-specific bugs.
#
BUILD_FROM_SRC :=
ifeq ($(OS),Windows_NT)
else
  UNAME_S := $(shell uname -s)
  ifeq ($(UNAME_S),Darwin)
    #BUILD_FROM_SRC += --build=b2
  endif
endif
#----- --- -- -  -  -   -

ifeq ($(BUILD_TYPE),)
BUILD_TYPE := RelWithDebInfo
endif

BUILD_DIR := build/$(BUILD_TYPE)


"$(BUILD_DIR)":
	mkdir -p "$(BUILD_DIR)"

build: "$(BUILD_DIR)"
	(cd $(BUILD_DIR) && conan build ../..)

test: "$(BUILD_DIR)"
ifeq ("$(GTEST_FILTER)","")
	@echo -e "\n\nRunning DEATH tests ==============================================\n"
	(cd $(BUILD_DIR) && GTEST_OUTPUT='xml:../death-test-results.xml' GTEST_FILTER='*Death*' ctest --verbose)
	@echo -e "\n\nRunning non-DEATH tests ==========================================\n"
	(cd $(BUILD_DIR) && GTEST_OUTPUT='xml:../test-results.xml' GTEST_FILTER='*-*Death*' ctest --verbose)
else
	(cd $(BUILD_DIR) && GTEST_OUTPUT='xml:../test-results.xml' ctest --verbose)
endif

install: "$(BUILD_DIR)"
	(cd "$(BUILD_DIR)" && conan install --profile "$(CONAN_PROFILE)" -s build_type=$(BUILD_TYPE) --build=missing $(BUILD_FROM_SRC) ../..)

create:
	(conan remove -f "llfs/$(shell script/get-version.sh)" && cd "$(BUILD_DIR)" && conan create  --profile "$(CONAN_PROFILE)" -s build_type=$(BUILD_TYPE) ../..)


publish:
	script/publish-release.sh


clean:
	rm -rf "$(BUILD_DIR)"


unlink:


.PHONY: rtags
rtags:
	rc -J  "$(BUILD_DIR)"
