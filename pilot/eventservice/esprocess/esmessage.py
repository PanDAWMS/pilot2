# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017

import logging
import os
import sys
import threading
import time
import traceback

from pilot.common.exception import PilotException, MessageFailure


logger = logging.getLogger(__name__)


class MessageThread(threading.Thread):
    """
    A thread to receive messages from payload and put recevied messages to the out queues.
    """

    def __init__(self, message_queue, socket_name=None, context='local', **kwds):
        """
        Initialize yampl server socket.

        :param message_queue: a queue to transfer messages between current instance and ESProcess.
        :param socket_name: name of the socket between current process and payload.
        :param context: name of the context between current process and payload, default is 'local'.
        :param **kwds: other parameters.

        :raises MessageFailure: when failed to setup message socket.
        """

        threading.Thread.__init__(self, **kwds)
        self.setName("MessageThread")
        self.__message_queue = message_queue
        self._socket_name = socket_name
        self.__stop = threading.Event()

        logger.info('try to import yampl')
        try:
            import yampl
        except Exception as e:
            raise MessageFailure("Failed to import yampl: %s" % e)
        logger.info('finished to import yampl')

        logger.info('start to setup yampl server socket.')
        try:
            if self._socket_name is None or len(self._socket_name) == 0:
                self._socket_name = 'EventService_EventRanges_' + str(os.getpid())
            self.__message_server = yampl.ServerSocket(self._socket_name, context)
        except Exception as e:
            raise MessageFailure("Failed to setup yampl server socket: %s %s" % (e, traceback.print_exc()))
        logger.info('finished to setup yampl server socket(socket_name: %s, context:%s).' % (self._socket_name, context))

    def get_yampl_socket_name(self):
        return self._socket_name

    def send(self, message):
        """
        Send messages to payload through yampl server socket.

        :param message: String of the message.

        :raises MessageFailure: When failed to send a message to the payload.
        """
        logger.debug('Send a message to yampl: %s' % message)
        try:
            if not self.__message_server:
                raise MessageFailure("No message server.")

            if (sys.version_info > (3, 0)):  # needed for Python 3
                message = message.encode('utf8')
            self.__message_server.send_raw(message)
        except Exception as e:
            raise MessageFailure(e)

    def stop(self):
        """
        Set stop event.
        """
        logger.debug('set stop event')
        self.__stop.set()

    def is_stopped(self):
        """
        Get status whether stop event is set.

        :returns: True if stop event is set, otherwise False.
        """
        return self.__stop.is_set()

    def terminate(self):
        """
        Terminate message server.
        """
        if self.__message_server:
            logger.info("Terminating message server.")
            del self.__message_server
            self.__message_server = None

    def run(self):
        """
        Main thread loop to poll messages from payload and
        put received into message queue for other processes to fetch.
        """
        logger.info('Message thread starts to run.')
        try:
            while True:
                if self.is_stopped():
                    self.terminate()
                    break
                if not self.__message_server:
                    raise MessageFailure("No message server.")

                size, buf = self.__message_server.try_recv_raw()
                if size == -1:
                    time.sleep(0.01)
                else:
                    if (sys.version_info > (3, 0)):  # needed for Python 3
                        buf = buf.decode('utf8')
                    self.__message_queue.put(buf)
        except PilotException as e:
            self.terminate()
            logger.error("Pilot Exception: Message thread got an exception, will finish: %s, %s" % (e.get_detail(), traceback.format_exc()))
            # raise e
        except Exception as e:
            self.terminate()
            logger.error("Message thread got an exception, will finish: %s" % str(e))
            # raise MessageFailure(e)
        self.terminate()
        logger.info('Message thread finished.')
