#!/usr/bin/env python2

import sys
import discoverparams
import readfactors
import random
import json
import os
import codecs
from datetime import date
from timeparameters import *
from calendar import timegm

PERSON_PREFIX = "http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers"
COUNTRY_PREFIX = "http://dbpedia.org/resource/"
SEED = 1

def findNameParameters(names, amount = 100):
	srtd = sorted(names,key=lambda x: -x[1])
	res = []
	hist = {}
	for t in srtd:
		if t[1] not in hist:
			hist[t[1]] = []
		hist[t[1]].append(t[0])
	counts = sorted([i for i in hist.iterkeys()])

	mid = len(counts)/2
	i = mid
	while counts[i] - counts[mid] < 0.1 * counts[mid]:
		res.extend([name for name in hist[counts[i]]])
		i += 1
	i = mid - 1
	while  counts[mid] - counts[i] < 0.1 * counts[mid]:
		res.extend([name for name in hist[counts[i]]])
		i -= 1
	return res

class CSVSerializer:
	def __init__(self):
		self.handlers = []
		self.inputs = []

	def setOutputFile(self, outputFile):
		self.outputFile=outputFile

	def registerHandler(self, handler, inputParams, header):
		handler.header = header
		self.handlers.append(handler)
		self.inputs.append(inputParams)

	def writeCSV(self):
		output = codecs.open( self.outputFile, "w",encoding="utf-8")

		if len(self.inputs) == 0:
			return

		headers = [self.handlers[j].header for j in range(len(self.handlers))]
		output.write("|".join(headers))
		output.write("\n")

		for i in range(len(self.inputs[0])):
			# compile a single CSV line from multiple handlers
			csvLine = []
			for j in range(len(self.handlers)):
				handler = self.handlers[j]
				data = self.inputs[j][i]
				csvLine.append(handler(data))
			output.write('|'.join([s for s in csvLine]))
			output.write("\n")
		output.close()

def handlePersonParam(person):
	return str(person)
	#return {"PersonID": person, "PersonURI":(PERSON_PREFIX+str("%020d"%person))}

def handleTimeParam(timeParam):
	#print timeParam.year
	#print timeParam.year
	res =  str(timegm(date(year=int(timeParam.year), 
		month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000)
	return res

def handleTimeDurationParam(timeParam):
	#print timeParam.year
	res =  str(timegm(date(year=int(timeParam.year), 
		month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000)
	res += "|"+str(timeParam.duration)
	return res


def handlePairCountryParam((Country1, Country2)):
	return Country1+"|"+Country2
	#return {"Country1":Country1, "Country2":Country2, "Country1URI":(COUNTRY_PREFIX + Country1), "Country2URI":(COUNTRY_PREFIX + Country2)}

def handleCountryParam(Country):
	return Country
	#return {"Country":Country, "CountryURI": (COUNTRY_PREFIX + Country)}

def handleTagParam(tag):
	return tag
	#return {"Tag": tag}

def handleTagTypeParam(tagType):
	return tagType
	#return {"TagType": tagType}

def handleHSParam((HS0, HS1)):
	return str(HS0)
	#return {"HS0":HS0, "HS1":HS1}

def handleFirstNameParam(firstName):
	return firstName
	#return {"Name":firstName}

def handlePairPersonParam((person1, person2)):
	return str(person1)+"|"+str(person2)
	#return {"Person1ID":person1, "Person2ID":person2, "Person2URI":(PERSON_PREFIX+str(person2)), "Person1URI":(PERSON_PREFIX+str(person1))}

def handleWorkYearParam(timeParam):
	return str(timeParam)
	#return {"Date0":timeParam}

def main(argv=None):
	if argv is None:
		argv = sys.argv

	if len(argv) < 3:
		print "arguments: <input dir> <output>"
		return 1

	indir = argv[1]+"/"
	activityFactorFiles=[]
	personFactorFiles=[]
	friendsFiles = []
	outdir = argv[2]+"/"
	random.seed(SEED)
	

	for file in os.listdir(indir):
		if file.endswith("activityFactors.txt"):
			activityFactorFiles.append(indir+file)
		if file.endswith("personFactors.txt"):
			personFactorFiles.append(indir+file)
		if file.startswith("m0friendList"):
			friendsFiles.append(indir+file)

	# read precomputed counts from files	
	(personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givenNames,  ts, postHisto) = readfactors.load(personFactorFiles, activityFactorFiles, friendsFiles)

	# find person parameters
	print "find parameter bindings for Persons"
	selectedPersonParams = {}
	for i in range(1, 15):
		factors = readfactors.getFactorsForQuery(i, personFactors)
		selectedPersonParams[i] = discoverparams.generate(factors)

	# Queries 13 and 14 take two person parameters each. Generate pairs
	secondPerson = {}
	for i in [13, 14]:
		secondPerson[i] = []
		for person in selectedPersonParams[i]:
			j = 0
			while True:
				j = random.randint(0, len(selectedPersonParams[i])-1)
				if selectedPersonParams[i][j] != person:
					break
			secondPerson[i].append(selectedPersonParams[i][j])

	# find country parameters for Query 3 and 11
	print "find parameter bindings for Countries"
	selectedCountryParams = {}
	for i in [3, 11]:
		factors = readfactors.getCountryFactorsForQuery(i, countryFactors)
		selectedCountryParams[i] = discoverparams.generate(factors, portion=0.1)

		# make sure there are as many country parameters as person parameters
		oldlen = len(selectedCountryParams[i])
		newlen = len(selectedPersonParams[i])
		selectedCountryParams[i].extend([selectedCountryParams[i][random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])

	# Query 3 needs two countries as parameters. Generate the second one:
	secondCountry = []
	for c in selectedCountryParams[3]:
		i=0
		while True:
			i = random.randint(0, len(selectedCountryParams[3])-1)
			if selectedCountryParams[3][i]!=c:
				break
		secondCountry.append(selectedCountryParams[3][i])

	#find tag parameters for Query 6
	#print "find parameter bindings for Tags"
  	# old tag selection
  	#selectedTagParams = {}
	#for i in [6]:
	#	selectedTagParams[i] = discoverparams.generate(tagFactors, portion=0.1)
	#	# make sure there are as many tag paramters as person parameters
	#	oldlen = len(selectedTagParams[i])
	#	newlen = len(selectedPersonParams[i])
	#	selectedTagParams[i].extend([selectedTagParams[i][random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])

	#print "find parameter bindings for Tags"
	(leftTagFactors, rightTagFactors) = discoverparams.divideFactors(tagFactors, 0.7)
	leftSize = len(leftTagFactors)
	rightSize = len(rightTagFactors)
	leftPortion = 0.1*(leftSize+rightSize) / (2.0*leftSize)
	rightPortion = 0.1*(leftSize+rightSize) / (2.0*rightSize)
	selectedTagParams = {}
	for i in [6]:
		selectedTagParams[i] = discoverparams.generate(leftTagFactors, portion=leftPortion)
		selectedTagParams[i].extend(discoverparams.generate(rightTagFactors, portion=rightPortion))
		oldlen = len(selectedTagParams[i])
		newlen = len(selectedPersonParams[i])
		selectedTagParams[i].extend([selectedTagParams[i][random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])

	# generate tag type parameters for Query 12
	selectedTagTypeParams = {}
	for i in [12]:
		selectedTagTypeParams[i] = discoverparams.generate(tagClassFactors, portion=0.1)
		# make sure there are as many tag paramters as person parameters
		oldlen = len(selectedTagTypeParams[i])
		newlen = len(selectedPersonParams[i])
		selectedTagTypeParams[i].extend([selectedTagTypeParams[i][random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])

	# find time parameters for Queries 2,3,4,5,9
	selectedPersons = selectedPersonParams[2] + selectedPersonParams[3]+selectedPersonParams[4]
	selectedPersons += selectedPersonParams[5] + selectedPersonParams[9]

	selectedTimeParams = {}
	timeSelectionInput = {
		2: (selectedPersonParams[2], "f", getTimeParamsBeforeMedian),
		3: (selectedPersonParams[3], "ff", getTimeParamsWithMedian),
		4: (selectedPersonParams[4], "f", getTimeParamsWithMedian),
		5: (selectedPersonParams[5], "ffg", getTimeParamsAfterMedian),
		9: (selectedPersonParams[9], "ff", getTimeParamsBeforeMedian)
		#11: (selectedPersonParams[11], "w", getTimeParamsBeforeMedian) # friends of friends work
	}

	print "find parameter bindings for Timestamps"
	selectedTimeParams = findTimeParams(timeSelectionInput, personFactorFiles, activityFactorFiles, friendsFiles, ts[1])
	# Query 11 takes WorksFrom timestamp
	selectedTimeParams[11] = [random.randint(ts[2], ts[3]) for j in range(len(selectedPersonParams[11]))]

	# Query 10 additionally needs the HS parameter
	HS = []
	for person in selectedPersonParams[10]:
		HS0 = random.randint(1, 12)
		if HS0 == 12:
			HS1 = 1
		else:
			HS1 = HS0 + 1
		HS.append((HS0, HS1))

	# Query 1 takes first name as a parameter
	#nameParams =  findNameParameters(nameFactors)# discoverparams.generate(nameFactors)
	## if there are fewer first names than person parameters, repeat some of the names
	#if len(nameParams) < len(selectedPersonParams[2]):
	#	oldlen = len(nameParams)
	#	newlen = len(selectedPersonParams[2])
	#	nameParams.extend([nameParams[random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])
	nameParams = []
	for person in selectedPersonParams[1]:
		n = givenNames.getValue(person)
		nameParams.append(n)

	# serialize all the parameters as CSV
	csvWriters = {}
	# all the queries have Person as parameter
	for i in range(1,15):
		csvWriter = CSVSerializer()
		csvWriter.setOutputFile(outdir+"interactive_%d_param.txt"%(i))
		if i != 13 and i != 14: # these three queries take two Persons as parameters
			csvWriter.registerHandler(handlePersonParam, selectedPersonParams[i], "Person")
		csvWriters[i] = csvWriter

	# add output for Time parameter
	for i in timeSelectionInput:
		if i==3 or i==4:
			csvWriters[i].registerHandler(handleTimeDurationParam, selectedTimeParams[i], "Date0|Duration")
		else:
			csvWriters[i].registerHandler(handleTimeParam, selectedTimeParams[i], "Date0")

	# other, query-specific parameters
	csvWriters[1].registerHandler(handleFirstNameParam, nameParams, "Name")
	csvWriters[3].registerHandler(handlePairCountryParam, zip(selectedCountryParams[3],secondCountry),"Country1|Country2")
	csvWriters[6].registerHandler(handleTagParam, selectedTagParams[6],"Tag")
	csvWriters[10].registerHandler(handleHSParam, HS, "HS0")
	csvWriters[11].registerHandler(handleCountryParam, selectedCountryParams[11],"Country")
	csvWriters[11].registerHandler(handleWorkYearParam, selectedTimeParams[11],"Year")
	csvWriters[12].registerHandler(handleTagTypeParam, selectedTagTypeParams[12],"TagType")
	csvWriters[13].registerHandler(handlePairPersonParam, zip(selectedPersonParams[13], secondPerson[13]),"Person1|Person2")
	csvWriters[14].registerHandler(handlePairPersonParam, zip(selectedPersonParams[14], secondPerson[14]),"Person1|Person2")


	for j in csvWriters:
		csvWriters[j].writeCSV()
	
if __name__ == "__main__":
	sys.exit(main())
