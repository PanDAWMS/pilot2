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

``-a``: Pilot work directory. This is the main work directory for the pilot. In this directory, the work directory
of the payload will be created (``./PanDA_Pilot2_%d_%s" % (`os.getpid()`, str(int(time.time())))``).

.. os.getpid(): https://docs.python.org/2/library/os.html#os.getpid
