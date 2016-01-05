#!/usr/bin/env python

import os
import sys

bs_dir, _ = os.path.split(__file__)
bs_dir = os.path.abspath(bs_dir)
toolchain_path = os.environ.get('TOOLCHAIN_CLONE',
                                '{0}/toolchain'.format(bs_dir))
sys.path.append(toolchain_path)
print(toolchain_path)

saved_cwd = os.getcwd()

try:
    os.chdir(toolchain_path)

    import toolchain

    deps = ['boost=1.57.0', 'thrift=0.9.2-p2', 'lz4=svn', 'snappy=1.0.5',
            'gperftools=2.3', 'googletest=20151222']

    os.environ['SYSTEM_GCC'] = '1'
    os.environ['DEBUG'] = '1'
    os.environ['SOURCE_DIR'] = os.getcwd()

    builder = toolchain.ScriptBuilder(deps)
    builder.build()
finally:
    os.chdir(saved_cwd)
