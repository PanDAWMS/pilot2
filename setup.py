#
#
# Setup for Pilot 2
#
#
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
    long_description='''This package contains the PanDA pilot 2 code''',
    license='Apache License 2.0',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://github.com/PanDAWMS/pilot2/wiki',
    python_requires='>=2.7',
    packages=find_packages(),
    install_requires=[], # TODO: check with Paul the dependencies
    data_files=[], # TODO: check with Paul if anything apart of the code needs to be installed
    scripts=[]
    )