#!/usr/bin/python
"""
Retrieves JMX data through locally running Jolokia server in a batch and prints them out in Diamond user script format.

Needs to be configured in main part of the script!
"""

import json
import urllib2
import re
import itertools

def flattenDicts(dicts):
    return {k: v for d in dicts for k, v in d.items()}	
		

class Request:
	"Request of JMX values for given MBean and attribute. Might result in multiple values (key, value pairs)."
	def __init__(self, type, mbean, attribute):
		self._type = type
		self._mbean = mbean
		self._attribute = attribute

	def asJsonString(self):
		data = {
			"type" : self._type,
			"mbean" : self._mbean,
			"attribute" : self._attribute
		}
		return json.dumps(data)

class Response:
	"Wrapper for a single response. Might contain multiple results."
	def __init__(self, data):
		request = data['request']
		
		self._type = request['type']
		self._mbean = request['mbean']
		self._attribute = request['attribute']

		self._value = data['value'] 

	def _sortedDesc(self, description):
		"Sort the description so that basic information (like type or host) comes first and it makes a usefull hierachical key in Graphite."
		result = []
		descriptions = dict(kv.split("=") for kv in description.split(","))
		
		if 'type' in descriptions:
			value = descriptions.pop('type')
			if value is not None:
				result.append('type=%s' % value)

		if 'host' in descriptions:
			value = descriptions.pop('host')
			if value is not None:
				result.append('host=%s' % value)
		
		for k, v in descriptions.items():
			result.append('%s=%s' % (k, v))

		return ','.join(result)

	def _name(self):
		(base, desc) = self._mbean.split(':')
		
		desc = self._sortedDesc(desc)

		mbean = "%s.%s" % (base, desc)	
		return re.sub('[=:,]', '.', mbean)

	def data(self):
		result = {}
		baseKey = "%s.%s" % (self._name(), self._attribute)

		if isinstance(self._value, dict):
			for k, v in self._value.iteritems():
				result['%s.%s' % (baseKey, k)] = v
		else:
			result[baseKey] = self._value

		return result

class Result:
	"Wrapper for results of a Jolokia request."
	def __init__(self, responses):
		self._responses = responses

	def asDict(self):
		return flattenDicts([response.data() for response in self._responses])	


class Jolokia:
	"Retrieves JMX data through Jolokia server running at given URL. Returns the result or passes it to handler function."
	def __init__(self, url):
		self._requests = []
		self._url = url

	def read(self, **kwargs):
		self._requests.append(Request('read', kwargs['mbean'], kwargs['attribute']))
		return self
	
	def _requestData(self):
		return "[%s]" % (', '.join([request.asJsonString() for request in self._requests]))

	def _readJson(self):
		req = urllib2.urlopen(urllib2.Request(self._url, self._requestData(), {'Content-Type': 'application/json'}))
		try:
			return req.read()
		finally:	
			req.close()

	def execute(self):
		return Result([Response(r) for r in json.loads(self._readJson())])

	def executeAndForwardTo(self, resultHandler):
		resultHandler(self.execute())


def printForDiamondUserScript(result):
	for k, v in result.asDict().items():
		print "%s %s" % (k, v)	

class JolokiaToDiamond:
	"Retrieves the requested JMX data through a locally running Jolokia server and prints it out in Diamond UserScript syntax."
	def __init__(self):
		pass

	def _print(self, result):
		for k, v in result.asDict().items():
			print "%s %s" % (k, v)	

	def port(self, port):
		self.port = port
		return self
	def get(self, *values):
		jolokia = Jolokia("http://localhost:%d/jolokia/" % self.port)

		for v in values:
			jolokia.read(**v)

		jolokia.executeAndForwardTo(self._print)


if __name__ == "__main__":

	#Jolokia("http://localhost:7777/jolokia/")\
	#	.read(mbean = "java.lang:type=Memory", attribute = "HeapMemoryUsage")\
	#	.read(mbean = "Catalina:type=Manager,context=/jsf-playground,host=localhost", attribute = "activeSessions")\
	#	.executeAndForwardTo(printForDiamondUserScript)

	JolokiaToDiamond().port(7777).get(*(
		{
			'mbean': 'java.lang:type=Memory',
			'attribute': 'HeapMemoryUsage'
		},
		{
			'mbean': 'Catalina:type=Manager,context=/jsf-playground,host=localhost',
			'attribute': 'activeSessions'
		}
	))	
	

