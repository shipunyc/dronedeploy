#!/usr/bin/env python

"""
The handler.
"""

import getopt
import time
import sys
import zmq


HEARTBEAT_DURATION_SECS = 1.0


def main(argv):
  try:
    opts, args = getopt.getopt(argv, '', ['manager=', 'id='])
  except getopt.GetoptError:
    print 'failed to get opts and args'
    sys.exit(2)
  opts = dict(opts)

  if not '--manager' in opts or not '--id' in opts:
    print 'missing args'
    sys.exit(2)
  manager_host_and_port = opts['--manager']
  handler_id = opts['--id']

  context = zmq.Context()

  socket = context.socket(zmq.REQ)
  socket.connect('tcp://' + manager_host_and_port)

  while True:
    socket.send(b'%s' % handler_id)
    #  Get the reply.
    message = socket.recv()
    if message == 'FREE':
      print 'I am free.'
    else:
      print 'I am with drone id %s' % message[3:]

    time.sleep(HEARTBEAT_DURATION_SECS)


if __name__ == '__main__':
  main(sys.argv[1:])
