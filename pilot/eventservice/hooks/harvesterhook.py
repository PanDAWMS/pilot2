#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017

"""
Hooks for Harvester EventService.
"""

from pilot.eventservice.eshook import ESHook


class HarvesterESHook(ESHook):
    def get_payload(self):
        """
        Get payload to execute.

        :returns: dict {'payload': <cmd string>, 'output_file': <filename or without it>, 'error_file': <filename or without it>}
        """
        raise Exception("Not Implemented")

    def get_event_ranges(self, num_ranges=1):
        """
        Get event ranges.

        :param num_ranges: Number of event ranges to download, default is 1.

        :returns: dict of event ranges.
                 None if no available events.
        """
        raise Exception("Not Implemented")

    def handle_out_message(self, message):
        """
        Handle ES output or error messages.

        :param message: a dict of parsed message.
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'finished', 'message': <full message>}.
        """
        raise Exception("Not Implemented")
