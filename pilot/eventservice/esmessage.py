# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017

import logging
import threading
import time
import traceback

from pilot.common.exception import PilotException, MessageFailure


logger = logging.getLogger(__name__)


class MessageThread(threading.Thread):
    """
    A thread to receive messages from payload and put recevied messages to the out queues.
    """

    def __init__(self, message_queue, socket_name='EventService_EventRanges', context='local', **kwds):
        """
        Initialize yampl server socket.
        """

        threading.Thread.__init__(self, **kwds)
        self.__message_queue = message_queue
        self._stop = threading.Event()

        logger.info('try to import yampl')
        try:
            import yampl
        except Exception as e:
            raise MessageFailure("Failed to import yampl: %s" % e)
        logger.info('finished to import yampl')

        logger.info('start to setup yampl server socket.')
        try:
            self.__message_server = yampl.ServerSocket(socket_name, context)
        except Exception as e:
            raise MessageFailure("Failed to setup yampl server socket: %s %s" % (e, traceback.print_exc()))
        logger.info('finished to setup yampl server socket.')

    def send(self, message):
        """
        Send messages to payload through yampl server socket.

        :param message: String of the message.
        :raises TODO:
        """
        logger.debug('Send a message to yampl: %s' % message)
        try:
            self.__message_server.send_raw(message)
        except Exception as e:
            raise MessageFailure(e)

    def stop(self):
        """
        Set stop event.
        """
        logger.debug('set stop event')
        self._stop.set()

    def stopped(self):
        """
        Get status whether stop event is set.

        :returns: True if stop event is set, otherwise False.
        """
        return self._stop.isSet()

    def terminate(self):
        logger.info('terminate message thread')
        del self.__message_server
        self.__message_server = None

    def run(self):
        """
        Main thread loop to receive messages from payload.
        """
        logger.info('Message thread starts to run.')
        try:
            while True:
                if self.stopped():
                    self.terminate()
                    break
                size, buf = self.__message_server.try_recv_raw()
                if size == -1:
                    time.sleep(0.00001)
                else:
                    self.__message_queue.put(buf)
        except PilotException as e:
            raise e
        except Exception as e:
            self.terminate()
            raise MessageFailure(e)
        logger.info('Message thread finished.')
