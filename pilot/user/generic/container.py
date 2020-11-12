#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2018

# import logging
# logger = logging.getLogger(__name__)


def do_use_container(**kwargs):
    """
    Decide whether to use a container or not.

    :param kwargs: dictionary of key-word arguments.
    :return: True is function has decided that a container should be used, False otherwise (boolean).
    """

    return True


def wrapper(executable, **kwargs):
    """
    Wrapper function for any container specific usage.
    This function will be called by pilot.util.container.execute() and prepends the executable with a container command.

    :param executable: command to be executed (string).
    :param kwargs: dictionary of key-word arguments.
    :return: executable wrapped with container command (string).
    """

    return executable


def create_stagein_container_command(workdir, cmd):
    """
    Create the stage-in container command.

    The function takes the isolated stage-in command, adds bits and pieces needed for the containerisation and stores
    it in a stagein.sh script file. It then generates the actual command that will execute the stage-in script in a
    container.

    :param workdir: working directory where script will be stored (string).
    :param cmd: isolated stage-in command (string).
    :return: container command to be executed (string).
    """

    return cmd
