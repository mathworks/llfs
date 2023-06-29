#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

export PROJECT_DIR := $(shell pwd)
PROJECT_NAME := llfs
SCRIPT_DIR := $(PROJECT_DIR)/script
CONAN_2 := 0

#----- --- -- -  -  -   -
.PHONY: all
all: install build test

include $(SCRIPT_DIR)/conan-targets.mk

TCMALLOC_ENV := $(shell find /lib/ -name '*tcmalloc.so*' | sort -Vr | head -1 | xargs -I{} echo LD_PRELOAD={})
$(info TCMALLOC_ENV=$(TCMALLOC_ENV))

#----- --- -- -  -  -   -
.PHONY: test
test:
ifeq ("$(GTEST_FILTER)","")
	@echo -e "\n\nRunning DEATH tests ==============================================\n"
	(cd "$(BUILD_DIR)" && GTEST_OUTPUT='xml:../death-test-results.xml' GTEST_FILTER='*Death*' $(TCMALLOC_ENV) ./llfs_Test)
	@echo -e "\n\nRunning non-DEATH tests ==========================================\n"
	(cd "$(BUILD_DIR)" && GTEST_OUTPUT='xml:../test-results.xml' GTEST_FILTER='*-*Death*' $(TCMALLOC_ENV) ./llfs_Test)
else
	(cd "$(BUILD_DIR)" && GTEST_OUTPUT='xml:../test-results.xml' $(TCMALLOC_ENV) ./llfs_Test)
endif

#----- --- -- -  -  -   -
.PHONY: publish
publish:
	script/publish-release.sh

#----- --- -- -  -  -   -
.PHONY: unlink
unlink:
	rm -f "$(BUILD_DIR)/llfs"
	rm -f "$(BUILD_DIR)/llfs_Test"

#----- --- -- -  -  -   -
.PHONY: rtags
rtags:
	rc -J  "$(BUILD_DIR)"
