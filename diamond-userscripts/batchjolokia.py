#!/usr/bin/python
#coding: utf8
"""
Retrieves JMX data through locally running Jolokia server in a batch and prints them out in Diamond user script format.

Needs to be configured in main part of the script!
"""

import json
import urllib2
import re
import itertools

class Collections:
	@classmethod
	def flattenDicts(cls, dicts):
		return {k: v for d in dicts for k, v in d.items()}

	@classmethod
	def isListOrTuple(cls, l):
		return isinstance(l, (list, tuple))

	@classmethod
	def flattenLists(cls, lists):
		return [ll for ll in l for l in lists if cls.isListOrTuple(l)] + [l for l in lists if not cls.isListOrTuple(l)]

class Request:
	"Request of JMX values for given MBean and attribute. Might result in multiple values (key, value pairs)."
	def __init__(self, type, mbean, attribute = None):
		self._type = type
		self._mbean = mbean
		self._attribute = attribute

	def asJsonString(self):
		data = {}
		data["type"] = self._type
		data["mbean"] = self._mbean
		if self._attribute is not None:
			data["attribute"] = self._attribute

		return json.dumps(data)

class Response:
	"Wrapper for a single response. Might contain multiple results."
	def __init__(self, data):
		request = data['request']
		
		self._type = request['type']
		self._mbean = request['mbean']
		self._attribute = request.get('attribute', None)

		self._value = data['value'] 

	@classmethod
	def of(cls, data):
		"Creates Response objects for the given data. Might result in multiple Response objects if the request included wildcards."
		request = data['request']
		type = request['type']

		if (type == 'read' and '*' in data['request']['mbean']):

			result = []

			for k, v in data['value'].items():
				mbean = k
				for attribute, value in v.items():
					subData = {
						'request': {
							'type': type,
							'mbean': mbean,
							'attribute': attribute
						},
						'value': value
					}
					result.append(cls(subData))
			return result
		
		return cls(data)

	def _sortedDesc(self, description):
		"Sort the description so that basic information (like type or host) comes first and it makes a usefull hierachical key in Graphite."
		
		if '*' in description:
			return description

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

		if 'context' in descriptions:
			value = descriptions.pop('context')
			if value is not None:
				if value == '/':
					value = 'ROOT'
				value = re.sub('[/]', '', value)	
				result.append('context=%s' % value)

		for k, v in descriptions.items():
			result.append('%s=%s' % (k, v))

		return ','.join(result)

	def _name(self):
		(base, desc) = self._mbean.split(':')
		
		desc = self._sortedDesc(desc)

		mbean = "%s.%s" % (base, desc)	
		return re.sub('[=:,]', '.', mbean)

	def _baseKey(self):
		name = self._name()
		if self._attribute is None:
			return "%s" % name
		return "%s.%s" % (name, self._attribute)

	def data(self):
		result = {}
		baseKey = self._baseKey()

		if isinstance(self._value, dict):
			for k, v in self._value.items():
				result['%s.%s' % (baseKey, k)] = v
		else:
			result[baseKey] = self._value

		return result

	def value(self):
		return self._value	

class Result:
	"Wrapper for results of a Jolokia request."
	def __init__(self, responses):
		self._responses = responses

	def asDict(self):
		return Collections.flattenDicts([response.data() for response in self._responses])

	def single(self):
		return self._responses[0]


class Jolokia:
	"Retrieves JMX data through Jolokia server running at given URL. Returns the result or passes it to handler function."
	def __init__(self, url):
		self._requests = []
		self._url = url

	def read(self, **kwargs):
		self._requests.append(Request('read', kwargs['mbean'], kwargs.get('attribute', None)))
		return self

	def search(self, **kwargs):
		self._requests.append(Request('search', kwargs['mbean']))
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
		return Result(Collections.flattenLists([Response.of(r) for r in json.loads(self._readJson())]))

	def executeAndForwardTo(self, resultHandler):
		resultHandler(self.execute())

def localJolokiaUrl(port):
	return "http://localhost:%d/jolokia/" % port

class JolokiaSearch:
	def __init__(self, **kwargs):
		self.port = kwargs['port']

	def search(self, search):
		return Jolokia(localJolokiaUrl(self.port)).search(mbean = search).execute().single().value()
			

class JolokiaToDiamond:
	"Retrieves the requested JMX data through a locally running Jolokia server and prints it out in Diamond UserScript syntax."
	def __init__(self, **kwargs):
		self.port = kwargs['port']

	def _print(self, result):
		for k, v in sorted(result.asDict().items()):
			print "jmx.%s %s" % (k, v)

	def get(self, *values):
		jolokia = Jolokia(localJolokiaUrl(self.port))

		for v in values:
			jolokia.read(**v)

		jolokia.executeAndForwardTo(self._print)

###################
### Configure below

if __name__ == "__main__":

	# Direct usage:
	#Jolokia("http://localhost:7777/jolokia/")\
	#	.read(mbean = "java.lang:type=Memory", attribute = "HeapMemoryUsage")\
	#	.read(mbean = "Catalina:type=Manager,context=/jsf-playground,host=localhost", attribute = "activeSessions")\
	#	.executeAndForwardTo(printForDiamondUserScript)

	# Discovery of possible requests:
	# [{'mbean': manager, 'attribute': 'activeSessions'} for manager in JolokiaSearch(port = 7777).search('Catalina:type=Manager,*') ]

	# Example configuration
	# attribute could be left out to retrieve all attributes of an mbean
	data = [
		### JVM
		# Memory
		{
			'mbean': 'java.lang:type=Memory',
			'attribute': 'HeapMemoryUsage'
		},
		{
			'mbean': 'java.lang:type=Memory',
			'attribute': 'NonHeapMemoryUsage'
		},
		# Threads
		{
			'mbean': 'java.lang:type=Threading',
			'attribute': 'ThreadCount'
		},


		### Tomcat
		# Example for usage of wildcards in mbean
		{
			"type" : "read",
			"mbean" : "*:context=*,host=localhost,type=Manager",
			"attribute" : "activeSessions"
		}
	]

	JolokiaToDiamond(port = 7777).get(*data)	
	

