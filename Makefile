#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

.PHONY: clean build build-nodoc install create test publish docker-build docker-push docker

CONAN_PROFILE := $(shell test -f /etc/conan_profile.default && echo '/etc/conan_profile.default' || echo 'default')
$(info CONAN_PROFILE is $(CONAN_PROFILE))


ifeq ($(BUILD_TYPE),)
BUILD_TYPE := RelWithDebInfo
endif

build:
	mkdir -p build/$(BUILD_TYPE)
	(cd build/$(BUILD_TYPE) && time -f "Build Time: %e seconds." conan build ../..)

test:
	mkdir -p build/$(BUILD_TYPE)
ifeq ("$(GTEST_FILTER)","")
	@echo -e "\n\nRunning DEATH tests ==============================================\n"
	(cd build/$(BUILD_TYPE) && GTEST_OUTPUT='xml:../death-test-results.xml' GTEST_FILTER='*Death*' ctest --verbose)
	@echo -e "\n\nRunning non-DEATH tests ==========================================\n"
	(cd build/$(BUILD_TYPE) && GTEST_OUTPUT='xml:../test-results.xml' GTEST_FILTER='*-*Death*' ctest --verbose)
else
	(cd build/$(BUILD_TYPE) && GTEST_OUTPUT='xml:../test-results.xml' ctest --verbose)
endif

install:
	mkdir -p build/$(BUILD_TYPE)
	(cd build/$(BUILD_TYPE) && conan install --profile "$(CONAN_PROFILE)" -s build_type=$(BUILD_TYPE) --build=missing ../..)

create:
	(conan remove -f "llfs/$(shell script/get-version.sh)" && cd build/$(BUILD_TYPE) && conan create  --profile "$(CONAN_PROFILE)" -s build_type=$(BUILD_TYPE) ../..)


publish:
	script/publish-release.sh


clean:
	rm -rf build/$(BUILD_TYPE)
