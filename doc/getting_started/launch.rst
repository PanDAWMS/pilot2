..
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Authors:
     - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

Launch pilot 2
==============

The pilot is a dependency-less Python application and relies on ``/usr/bin/env python``. The minimum pilot can be
called like:

.. code-block:: shell

    ./pilot.py -d -q <QUEUE_NAME>

The ``QUEUE_NAME`` correspond to the ATLAS PandaQueue as defined in `AGIS`_. This will launch the default ``generic``
workflow with lifetime default lifetime of 10 seconds (i.e., too short to do anything).

.. _AGIS: http://atlas-agis.cern.ch/agis/

The ``-d`` argument changes the logger to produce debug output.