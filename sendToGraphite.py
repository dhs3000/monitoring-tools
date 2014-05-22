#!/usr/bin/env python
#coding: utf8
"""
Reads all lines from STDIN and sends them to the given Graphite server.
The lines are prefixed by the given prefix and this hostname and extended by the current timestamp.

Arguments:
--prefix
--server hostname of the Graphite server
--port 
"""

class GraphiteSender:
	def __init__(self, **kwargs):
		self._server = kwargs['server']
		self._port = kwargs['port']
		self._prefix = kwargs['prefix']

	def send(self, messages):
		import socket
		import time

		timestamp = int(time.time())
		messages_ = ['%s.%s %d' %(self._prefix, m, timestamp) for m in messages]

		if not messages_:
			return

		print 'sending messages:'
		for m in messages_:
			print ' => %s' % m

		sock = socket.socket()
		sock.connect((self._server, self._port))
		sock.sendall('\n'.join(messages_) + '\n')
		sock.close()

class Application:
	def __init__(self, argv):
		from getopt import getopt
		from platform import node
		
		longopts, shortopts = getopt(argv, shortopts = '', longopts = ['prefix=', 'server=', 'port='])
		args = dict(longopts)

		self._prefix = args.get('--prefix', 'send-to-graphite-script')
		self._server = args.get('--server', 'graphite')
		self._port = int(args.get('--port', 2003))

		print "Sending data to '%s:%d' with prefix '%s' ..." % (self._server, self._port, self._prefix)

		self._node = node().replace('.', '-')

	def _std_lines(self):
		import sys
		return (l.strip() for l in sys.stdin)

	def run(self):	
		lines = self._std_lines()
		GraphiteSender(server = self._server, port = self._port, prefix = ('%s.%s' % (self._prefix, self._node))).send(lines)  

if __name__ == '__main__':
	import sys
	Application(sys.argv[1:]).run()