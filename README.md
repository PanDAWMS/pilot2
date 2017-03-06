# PanDA Pilot - Architecture 2

## Contributions

1. Check the ``TODO.md`` file.

2. Fork the ``PanDAWMS/pilot2`` repository into your private account as ``origin``. Clone it and set the ``PanDAWMS/pilot2`` repository as ``upstream``.

3. Make new code contributions only to a new branch in your repository, push to ``origin`` and make a pull request into ``upstream``. Depending on the type of contribution this should go against either ``upstream/master``, ``upstream/main-dev`` or ``upstream/hotfix``.

## Verifying code correctness

Do not submit code that does not conform to the project standards. We use PEP8 and Flake verification, with everything enabled at a maximum line length of 160 characters and McCabe complexity 12:

    flake8 pilot.py pilot/

For Python 2.6 you need to install ``flake8<3.0.0``.

## Running the pilot

The pilot is a dependency-less Python application and relies on ``/usr/bin/env python``. The minimum pilot can be called like:

    ./pilot.py -s <SITE_NAME> -r <RESOURCE_NAME> -q <QUEUE_NAME> -l 60

where ``SITE_NAME``, ``RESOURCE_NAME`` ``QUEUE_NAME`` correspond to the ATLAS SiteName, PanDA Resource, PandaQueue. This will launch the default ``generic`` workflow with lifetime 60 seconds.

The ``-d`` argument will change the logger to more verbose output.

## Running the testcases

The test cases are implemented as standard Python unittests under directory ``pilot/test/``. They can be discovered and executed automatically:

    python -m unittest discover -v

For Python 2.6 you need to install the ``unittest2``, and call appropriately:

    python -m unittest2 discover -v
