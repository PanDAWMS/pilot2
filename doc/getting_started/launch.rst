..
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Authors:
     - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
     - Paul Nilsson, paul.nilsson@cern.ch, 2017

Launch pilot 2
==============

The pilot is a dependency-less Python application and relies on ``/usr/bin/env python``. The minimum pilot can be
called like:

.. code-block:: shell

    ./pilot.py -d -q <QUEUE_NAME>

The ``QUEUE_NAME`` correspond to the ATLAS PandaQueue as defined in `AGIS`_. This will launch the default ``generic``
workflow with lifetime default lifetime of 10 seconds.

.. _AGIS: http://atlas-agis.cern.ch/agis/

The ``-d`` argument changes the logger to produce debug output.

The current range of implemented pilot options is:

``-a``: Pilot work directory (string; full path). This is the main work directory for the pilot. In this directory, the
work directory of the payload will be created (``./PanDA_Pilot2_%d_%s" % (os.getpid(), str(int(time.time())))``).

``-d``: Enable debug mode for logging messages. No value should be specified.

``-w``: Desired workflow (string). Default is ``generic``, which currently means stage-in, payload execution and
stage-out will be performed. Other workflows to be defined. The workflow name should match an existing module in the
``workflow`` Pilot 2 directory.

``-l``: Lifetime in seconds (integer). Default during Pilot 2 testing and implementation stage is currently 3600 s. It
will be increased at a later time.

``-q``: PanDA queue name (string). E.g. AGLT2_TEST-condor.

``-s``: PanDA site name (string). E.g. AGLT2_TEST. Note: the site name is only necessary for the dispatcher. The pilot
will send it to the dispatcher with the getJob command.

``-j``: Job label (string). A prod/source label which currently has default value `ptest`. For production jobs, set
this to `managed` while `user` is the value for user jobs. Setting it to `test` will result in a test job.

``--cacert``: CA certificate to use with HTTPS calls to server, commonly X509 proxy (string). Not needed on the grid.

``--capath``: CA certificates path (string). Not needed on the grid.

``--url``: PanDA server URL (string). Default is `https://pandaserver.cern.ch`.

``-p``: PanDA server port (integer). Default is `25443`.

``--config``: Config file path (string). Path to `pilot_config.cfg` file.

``--country_group``: Country group option for getjob request (string).

``--working_group``: Working group option for getjob request (string).

``--allow_other_country``: Is the resource allowed to be used outside the privileged group (boolean)?

``--allow_same_user``: Multi-jobs will only come from same taskID and thus same user (boolean).

``--pilot_user``: Pilot user (string). E.g. name of experiment.









