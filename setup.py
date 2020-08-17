#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Fernando Barreiro Megino, fernando.harald.barreiro.megino@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-2020
import sys

from setuptools import setup, find_packages

sys.path.insert(0, '.')

# get release version
with open('PILOTVERSION') as reader:
    release_version = reader.read()

setup(
    name="panda-pilot",
    version=release_version,
    description='PanDA Pilot 2',
    long_description='''This package contains the PanDA Pilot 2 source code''',
    license='Apache License 2.0',
    author='PanDA Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://github.com/PanDAWMS/pilot2/wiki',
    python_requires='>=2.7',
    packages=find_packages(),
    install_requires=[],
    data_files=[],
    include_package_data=True,
    scripts=['pilot.py']
    )