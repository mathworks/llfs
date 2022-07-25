.PHONY: build build-nodoc install create test publish docker-build docker-push docker

ifeq ($(BUILD_TYPE),)
BUILD_TYPE := RelWithDebInfo
endif

build: | install
	mkdir -p build/$(BUILD_TYPE)
	(cd build/$(BUILD_TYPE) && time -f "Build Time: %e seconds." conan build ../..)

test: build
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
	(cd build/$(BUILD_TYPE) && conan install ../.. -s build_type=$(BUILD_TYPE) --build=missing)

create:
	(conan remove -f "llfs/$(shell batteries/script/get-version.sh)" && cd build/$(BUILD_TYPE) && conan create ../.. -s build_type=$(BUILD_TYPE))


publish: | test build
	batteries/script/publish-release.sh


docker-build:
	(cd docker && docker build -t registry.gitlab.com/tonyastolfi/batteries .)


docker-push: | docker-build
	(cd docker && docker push registry.gitlab.com/tonyastolfi/batteries)


docker: docker-build docker-push
