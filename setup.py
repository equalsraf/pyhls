#!/usr/bin/env python3

from distutils.core import setup

setup(name='pyhls',
        version='0.1',
        description='python hls module',
        py_modules=['hls', 'm3u', 'hlsdump'],
        scripts=['hlsdump.py']
        )
