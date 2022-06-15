.PHONY: install build test clean

ifeq ($(BUILD_TYPE),)
BUILD_TYPE := RelWithDebInfo
endif

install:
	./configure $(BUILD_TYPE)

build:
	cd build/$(BUILD_TYPE) && conan build ../..

test: build
	build/$(BUILD_TYPE)/bin/turtle_util_Test
	build/$(BUILD_TYPE)/bin/llfs_Test
	build/$(BUILD_TYPE)/bin/turtle_core_Test
	build/$(BUILD_TYPE)/bin/turtle_tree_Test
	build/$(BUILD_TYPE)/bin/turtle_db_Test

clean:
	rm -rf build/$(BUILD_TYPE)
