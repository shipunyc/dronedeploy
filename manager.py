#!/usr/bin/env python

"""
The manager.
"""

import getopt
import time
import sys
import zmq


MAX_IDLE_SECS = 10.0
PULLER_TIMEOUT_SECS = 1.0


class Manager(object):
  """The manager object.
  """

  def __init__(self, drones_host_and_port, handlers_host_and_port):
    """Initialization.
    """
    context = zmq.Context()

    self._drones_socket = context.socket(zmq.REP)
    self._drones_socket.bind('tcp://' + drones_host_and_port)

    self._handlers_socket = context.socket(zmq.REP)
    self._handlers_socket.bind('tcp://' + handlers_host_and_port)

    # Initialize poll set
    self._poller = zmq.Poller()
    self._poller.register(self._drones_socket, zmq.POLLIN)
    self._poller.register(self._handlers_socket, zmq.POLLIN)

    # Records the drones and handlers with the following lists and dicts.
    self._drones_heartbeat = {}  # drone ids are keys
    self._handlers_heartbeat = {}  # handler ids are keys
    self._drones_to_handlers = {}  # drone ids are keys
    self._handlers_to_drones = {}  # handler ids are keys
    self._available_handlers = []
    self._drones_to_drop = []

  def start(self):
    """This is the main loop.
    """
    print 'manager started (%d available handlers)' % len(self._available_handlers)
    while True:
      try:
        all_sockets = dict(self._poller.poll(PULLER_TIMEOUT_SECS * 1000))
      except KeyboardInterrupt:
        break
      self._remove_non_existing_handlers_and_drones()
      self._process_handlers_socket(all_sockets)
      self._process_drones_socket(all_sockets)

  def _remove_non_existing_handlers_and_drones(self):
    """Remove handlers or drones who are actually gone.
    """
    # Remove non-existing handlers.
    for handler_id in (self._available_handlers + list(self._handlers_to_drones)):
      idle_secs = time.time() - self._handlers_heartbeat[handler_id]
      if idle_secs > MAX_IDLE_SECS:
        self._handlers_heartbeat.pop(handler_id, None)
        if handler_id in self._handlers_to_drones:
          # Let the drone on the handler drop.
          drone_id = self._handlers_to_drones[handler_id]
          self._drones_heartbeat.pop(drone_id, None)
          self._drones_to_handlers.pop(drone_id, None)
          self._handlers_to_drones.pop(handler_id, None)
          # Keep it in the list, so that we can notify the drone later.
          self._drones_to_drop.append(drone_id)
          print ('no heartbeat from drone_hander id=%s received for %0.1f ' +
                 'seconds, dropping and dropping drone id=%s (%d ' +
                 'available handler)') % (
                 handler_id, idle_secs, drone_id, len(self._available_handlers))
        else:
          self._available_handlers.remove(handler_id)
          print ('no heartbeat from drone_hander id=%s received for %0.1f ' +
                 'seconds, (%d available handlers)') %(
                handler_id, idle_secs, len(self._available_handlers))
    # Remove non-existing drones.
    for drone_id in list(self._drones_to_handlers):
      idle_secs = time.time() - self._drones_heartbeat[drone_id]
      if idle_secs > MAX_IDLE_SECS:
        self._drones_heartbeat.pop(handler_id, None)
        # Free the handler.
        if drone_id in self._drones_to_handlers:
          handler_id = self._drones_to_handlers[drone_id]
          self._available_handlers.append(handler_id)
          self._handlers_to_drones.pop(handler_id, None)
          self._drones_to_handlers.pop(drone_id, None)
          print ('no heartbeat from drone id=%s received for %0.1f seconds, '+
                 'dropping and freeing drone_handler id=%s (%d ' +
                 'available handlers)') % (
                 drone_id, idle_secs, handler_id, len(self._available_handlers))

  def _process_handlers_socket(self, all_sockets):
    """Process connetions to self._handlers_socket
    """
    if self._handlers_socket in all_sockets:
      handler_id = self._handlers_socket.recv()
      if (not handler_id in self._handlers_to_drones and
          not handler_id in self._available_handlers):
        # This is a new handler.
        self._available_handlers.append(handler_id)
        print 'drone_handler with id=%s connected (%d available handlers)' % (
              handler_id, len(self._available_handlers))
        self._handlers_socket.send(b'FREE')
      elif handler_id in self._handlers_to_drones:
        # This handler has a drone on it.
        drone_id = self._handlers_to_drones[handler_id]
        self._handlers_socket.send(b'OK+%s' % drone_id)
      else:
        # This is an existing available handler.
        self._handlers_socket.send(b'FREE')
      self._handlers_heartbeat[handler_id] = time.time()

  def _process_drones_socket(self, all_sockets):
    """Process connections to self._drones_socket
    """
    if self._drones_socket in all_sockets:
      drone_id = self._drones_socket.recv()
      if drone_id in self._drones_to_drop:
        # The drone's handler left.
        self._drones_to_drop.remove(drone_id)
        self._drones_socket.send(b'DROP')
      elif not drone_id in self._drones_to_handlers:
        # This is a newly arrived drone.
        if self._available_handlers:
          # We have a handler for it.
          assigned_handler_id = self._available_handlers.pop()
          self._drones_to_handlers[drone_id] = assigned_handler_id
          self._handlers_to_drones[assigned_handler_id] = drone_id
          self._drones_heartbeat[drone_id] = time.time()
          print ('drone with id=%s connected and assigned to drone_handler ' +
                 'id=%s (%d available handlers)') % (
                 drone_id, assigned_handler_id, len(self._available_handlers))
          self._drones_socket.send(b'OK+%s' % assigned_handler_id)
        else:
          # No handlers are available,
          print ('drone with id=%s connected, dropping connection '+
                 'because no available drone_handlers') % drone_id
          self._drones_socket.send(b'DROP')
      else:
        handler_id = self._drones_to_handlers[drone_id]
        # The drone is in good status.
        self._drones_heartbeat[drone_id] = time.time()
        self._drones_socket.send(b'OK+%s' % handler_id)


def main(argv):
  try:
    opts, args = getopt.getopt(argv, '', ['listen_drones=', 'listen_handlers='])
  except getopt.GetoptError:
    print 'failed to get opts and args'
    sys.exit(2)
  opts = dict(opts)

  if not '--listen_drones' in opts or not '--listen_handlers' in opts:
    print 'missing args'
    sys.exit(2)
  drones_host_and_port = opts['--listen_drones']
  handlers_host_and_port = opts['--listen_handlers']

  manager = Manager(drones_host_and_port, handlers_host_and_port)
  manager.start()


if __name__ == '__main__':
  main(sys.argv[1:])
