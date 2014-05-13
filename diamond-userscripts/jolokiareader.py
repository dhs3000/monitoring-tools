#!/usr/bin/python
"""
Prints out Jolokia JMX information in Diamond user script format.

Needs to be configured!
"""

import sys
import re
import urllib2
import json
import time

from collections import namedtuple

Constants = namedtuple('Constants', ['KEY_PREFIX'])('jmx')


class UrlJolokiaDataRetriever:
	def __init__(self, url):
		self.url = url

	def get(self):
		con = urllib2.urlopen(self.url)
		return json.loads(con.read())

class MockedJolokiaDataRetriever:
	def get(self):
		return """
			{
				"timestamp":1399359113,
				"status":200,
				"request": {
					"mbean":"java.lang:type=Memory",
					"type":"read"
				},
				"value": {
					"Verbose":false,
					"ObjectPendingFinalizationCount":0,
					"NonHeapMemoryUsage": {
						"max":587202560,
						"committed":371851264,
						"init":24313856,
						"used":317305280
					},
					"HeapMemoryUsage": {
						"max":2863333376,
						"committed":1999896576,
						"init":64665024,
						"used":1721502960
					},
					"ObjectName": {
						"objectName":"java.lang:type=Memory"
					}
				}
			}
			"""

class JolokiaJsonDataExtractor:
	"Extracts the data for the given keys from Jolokia (JSON) Object"
	def __init__(self, *keys):
		self.keys = keys

	def extract(self, jsonData):

		requestedData = jsonData['value']
		result = {}

		result['mbean'] = jsonData['request']['mbean']
		data = result['data'] = {}

		for whole_key in self.keys:
			value = None

			for key in re.split(r'\.', whole_key):
				if value is None:
					value = requestedData[key]
				else:
					value = value[key]

			data[whole_key] = value

		return result

class DiamondPrintForUserScriptDataHandler:
	"Prints out the given data as Diamond user script to collect information."
	def __init__(self, keys, result):
		self.keys = keys
		self.mbean = result['mbean']
		self.data = result['data']

	def __name(self):
		return re.sub('[=:]', '.', self.mbean)

	def __graphiteKey(self, key):
		return "%s.%s.%s" % (Constants.KEY_PREFIX, self.__name(), key)

	def __graphiteValue(self, key):
		return "%s" % (self.data[key])

	def __graphiteString(self, key):
		#return "%s=%s %d" % (self.__graphiteKey(key), self.__graphiteValue(key), int(time.time()))
		return "%s %s" % (self.__graphiteKey(key), self.__graphiteValue(key))

	def handle(self):
		for key in self.keys:
			print self.__graphiteString(key)
		

class DataHandlerBuilder:
	"Builds a DataHandler with the given constructor and the keys."
	def __init__(self, dataHandlerConstructor, *keys):
		self.dataHandlerConstructor = dataHandlerConstructor
		self.keys = keys

	def build(self, data):
		return self.dataHandlerConstructor(self.keys, data)


class JolokiaReader:
	"Reading Jolokia information from a given Retriever, extracting data with given Extractor and passing it to a given handler."
	def __init__(self, argv=sys.argv):
		self.argv = argv

	def run(self, dataRetriever, dataExtractor, dataHandlerBuilder):
		jsonData = json.loads(dataRetriever.get())
		data = dataExtractor.extract(jsonData)
		dataHandlerBuilder.build(data).handle()

# ==== Configure below, what to read from where etc.
# As Diamond's user script collector reads line-wise, one could have a couple of differnt app.run's
if __name__ == "__main__":
	app = JolokiaReader()

	baseUrl = 'http://localhost:39335/jolokia'
	action = 'read'
	jmxBean = 'java.lang:type=Memory'

	keys = ("HeapMemoryUsage.max", "HeapMemoryUsage.used")

	retriever = UrlJolokiaDataRetriever('%s/%s/%s' % (baseUrl, action, jmxBean))
	#retriever = MockedJolokiaDataRetriever()
	extractor = JolokiaJsonDataExtractor(*keys)
	handlerBuilder = DataHandlerBuilder(DiamondPrintForUserScriptDataHandler, *keys)

 	app.run(retriever, extractor, handlerBuilder)
