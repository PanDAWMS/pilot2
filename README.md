# PanDA Pilot - minipilot for Harvester ManyToOne operations

## Contributions

1. Check the ``TODO.md`` and ``STYLEGUIDE.md`` files.

2. Fork the ``PanDAWMS/pilot2`` repository into your private account as ``origin``. Clone it and set the ``PanDAWMS/pilot2`` repository as ``upstream``.

3. Make new code contributions only to a new branch in your repository, push to ``origin`` and make a pull request into ``upstream``. Depending on the type of contribution this should go against either ``upstream/master``, ``upstream/main-dev`` or ``upstream/hotfix``.

## Verifying code correctness

Do not submit code that does not conform to the project standards. We use PEP8 and Flake verification, with everything enabled at a maximum line length of 160 characters and McCabe complexity 12:

    flake8 pilot.py pilot/

For Python 2.6 you need to install ``flake8<3.0.0``, which can miss a few things. Check the output of TravisCI to verify if you have to use this old version.

## Running the pilot

The pilot is a dependency-less Python application and relies on ``/usr/bin/env python``. The minimum pilot can be called like:
    
    python ./pilot.py --queue <QUEUE_NAME> --queuedata <QUEUE_JSON_FILE> --job_tag prod --job_description <JOB_SPEC_JSON> --simulate_rucio --no_job_update --harvester --harvester_workdir <JOB_WORKING_DIR> --harvester_datadir <LOCAL_RUCIO_DIR> --harvester_eventStatusDumpJsonFile <EVNT_STAT_JSON> --harvester_workerAttributesFile <WORK_ATTR_JSON>
   

where 

   ``QUEUE_NAME`` - the ATLAS PandaQueue as defined in AGIS
   ``QUEUE_JSON_FILE`` - the AGIS response for this queue, created by Harvester
   ``JOB_SPEC_JSON`` - a file containing the PanDA job description in json format, created by Harvester
   ``JOB_WORKING_DIR`` - the directory where this pilot should run, nominal location of input and output files
   ``LOCAL_RUCIO_DIR`` - local directory which follows the rucio DATADISK structure where input and output files will be stored
   ``EVNT_STAT_JSON`` - this is the file used to communicate event status to Harvester, file defined in harvester config ``payload_interaction.eventStatusDumpJsonFile``
   ``WORK_ATTR_JSON`` - this is the file used to communicate the job status to Harvester, file defined in harvester config ``payload_interaction.workerAttributesFile``



