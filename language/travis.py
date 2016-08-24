#!/usr/bin/env python

# Copyright 2016 Stanford University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os, platform, subprocess

def test(root_dir, debug, spy, env):
    threads = ['-j', '2'] if 'TRAVIS' in env else []
    terra = ['--with-terra', env['TERRA_DIR']] if 'TERRA_DIR' in env else []
    debug_flag = ['--debug'] if debug else []
    inner_flag = ['--extra=-flegion-inner', '--extra=0'] if 'DISABLE_INNER' in env else []

    subprocess.check_call(
        ['time', './install.py', '--rdir=auto'] + threads + terra + debug_flag,
        env = env,
        cwd = root_dir)
    if not spy:
        subprocess.check_call(
            ['time', './test.py', '-q'] + threads + debug_flag + inner_flag,
            env = env,
            cwd = root_dir)
    if spy:
        subprocess.check_call(
            ['time', './test.py', '-q', '--spy'] + threads + inner_flag,
            env = env,
            cwd = root_dir)

if __name__ == '__main__':
    root_dir = os.path.realpath(os.path.dirname(__file__))
    legion_dir = os.path.dirname(root_dir)
    runtime_dir = os.path.join(legion_dir, 'runtime')

    env = dict(os.environ.iteritems())
    env.update({
        'LG_RT_DIR': runtime_dir,
        'LUAJIT_URL': 'http://legion.stanford.edu/~eslaught/mirror/LuaJIT-2.0.4.tar.gz',
    })
    # reduce output spewage by default
    if 'MAKEFLAGS' not in env:
        env['MAKEFLAGS'] = 's'

    test(root_dir, env['DEBUG'], 'TEST_SPY' in env and env['TEST_SPY'], env)
