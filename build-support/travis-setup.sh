#!/usr/bin/env bash

if [ "$TRAVIS_OS_NAME" = "linux" ]; then
	export NATIVE_TOOLCHAIN=`pwd`/toolchain
	./build-support/linux-toolchain.py
fi

if [ "$TRAVIS_OS_NAME" = "osx" ]; then
	pushd build-support
	if [ ! -d "toolchain" ]; then
		git clone https://github.com/wesm/native-toolchain.git toolchain
		pushd toolchain
		git checkout python-dep-manager
		popd
	fi
	./osx-toolchain.py
	popd
	ln -s `pwd`/build-support/toolchain/build toolchain
fi
