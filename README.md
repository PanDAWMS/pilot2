# PanDA Pilot 2

## Contributions

1. Check the ``TODO.md`` and ``STYLEGUIDE.md`` files.

2. Fork the ``PanDAWMS/pilot2`` repository into your private account as ``origin``. Clone it and set the ``PanDAWMS/pilot2`` repository as ``upstream``.

3. Make new code contributions only to a new branch in your repository, push to ``origin`` and make a pull request into ``upstream``. Depending on the type of contribution this should go yo either ``upstream/next`` or ``upstream/hotfix``. 
   Any pull requests directly to the master branch will be rejected since that would trigger the automatic pilot tarball creation.
## Verifying code correctness

Do not submit code that does not conform to the project standards. We use PEP8 and Flake verification, with everything
enabled at a maximum line length of 160 characters and McCabe complexity 12, as well Pylint:

    flake8 pilot.py pilot/
    pylint <path to pilot module>

## Running the pilot

The pilot is a dependency-less Python application and relies on ``/usr/bin/env python``. The minimum pilot can be called like:

    ./pilot.py -q <PANDA_QUEUE>

where ``PANDA_QUEUE`` correspond to the ATLAS PandaQueue as defined in AGIS. This will launch the default ``generic`` workflow.

## Running the testcases

The test cases are implemented as standard Python unittests under directory ``pilot/test/``. They can be discovered and executed automatically:

    unit2 -v

## Building and viewing docs

1. Install ``sphinx`` into your environment by ``pip`` or other means with all the necessary requirements.

2. Navigate into ``./doc`` in your fork and run ``make html``.

3. Open ``_build/html/index.html`` with your browser.

### Automate documentation to your module

For automatic code documentation of any new pilot module, add the following lines to the corresponding rst file in the doc area:

    .. automodule:: your.module
        :members:

See existing rst files. For more info, visit http://sphinx-doc.org

### Syncing your GitHub repository

Before making a pull request, make sure that you are synced to the latest version.

1. git clone https://github.com/USERNAME/pilot2.git
2. cd pilot2
3. git checkout next
4. git remote -v
5. git remote add upstream https://github.com/PanDAWMS/pilot2.git
6. git fetch upstream
7. git merge upstream/next
