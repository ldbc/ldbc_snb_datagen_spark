import sys
import getopt
import codecs

FACTORS = ["f", "ff", "fp", "fpt", "ffg", "p", "pl", "pt", "pr", "g", "w", "ffw", "ffp", "fw", "fg", "ffpt", "fpr", "org"]

FACTOR_MAP = {value: key for (key, value) in enumerate(FACTORS)}

class FactorCount:
	def __init__(self):
		self.values = [0]*len(FACTORS)

	def setValue(self, factor, value):
		self.values[FACTOR_MAP[factor]] = value

	def addValue(self, factor, value):
		self.values[FACTOR_MAP[factor]] += value

	def getValue(self, factor):
		return self.values[FACTOR_MAP[factor]]

class Factors:
	def __init__(self, persons = []):
		self.values = {}
		for p in persons:
			self.values[p] = FactorCount()

	def addNewParam(self, p):
		self.values[p] = FactorCount()

	def setValue(self, person, factor, value):
		self.values[person].setValue(factor, value)

	def getValue(self, person, factor):
		return self.values[person].getValue(factor)

	def addValue(self, person, factor, value):
		self.values[person].addValue(factor, value)


def load(inputFileName, inputFriendsFileName):
	print "loading input for parameter generation"
	results = Factors()
	countries = Factors()
	tagClasses = []
	tags = []
	names = []
	timestamp = [0,0,0,0]

	with codecs.open(inputFileName, "r", "utf-8") as f:
		line = f.readline()

		personCount = int(line)

		for i in range(personCount):
			line = f.readline().split(",")
			person = int(line[0])
			results.addNewParam(person)
			results.setValue(person, "f", int(line[1]))
			results.setValue(person, "p", int(line[2]))
			results.setValue(person, "pl", int(line[3]))
			results.setValue(person, "pt", int(line[4]))
			results.setValue(person, "g", int(line[5]))
			results.setValue(person, "w", int(line[6]))
			results.setValue(person, "pr", int(line[7]))

		countryCount = int(f.readline())

		for i in range(countryCount):
			line = f.readline().split(",")
			country = line[0]
			countries.addNewParam(country)
			countries.setValue(country, "p", int(line[1]))

		tagCount = int(f.readline())

		for i in range(tagCount):
			line = f.readline().split(",")
			tag = line[0]
			tagClasses.append([tag, int(line[2])])
			
		tagCount = int(f.readline())
		for i in range(tagCount):
			line = f.readline()
			count = line[1+line.rfind(","):]
			name = line[:line.rfind(",")]
			tags.append([name, int(count)])

		nameCount = int(f.readline())

		for i in range(nameCount):
			line = f.readline().split(",")
			nameFactor = [line[0]]
			nameFactor.append(int(line[1]))
			names.append(nameFactor)

		for i in range(4):
			timestamp[i]  = int(f.readline())

	loadFriends(inputFriendsFileName, results)

	return (results, countries, tags, tagClasses, names, timestamp)

def loadFriends(inputFriendsFileName, factors):

	# scan the friends list and sum up the counts related to friends (number of posts of friends etc)
	with open(inputFriendsFileName, 'r') as f:
		for line in f:
			people = map(int, line.split(","))
			person = people[0]
			for friend in people[1:]:
				factors.addValue(person, "ff", factors.getValue(friend, "f"))
				factors.addValue(person, "fp", factors.getValue(friend, "p"))
				factors.addValue(person, "fpt", factors.getValue(friend, "pt"))
				factors.addValue(person, "fw", factors.getValue(friend, "w"))
				factors.addValue(person, "fg", factors.getValue(friend, "g"))

	# second scan for friends-of-friends counts (groups of friends of friends)
	with open(inputFriendsFileName, 'r') as f:
		for line in f:
			people = map(int, line.split(","))
			person = people[0]
			for friend in people[1:]:
				factors.addValue(person, "ffg", factors.getValue(friend, "fg"))
				factors.addValue(person, "ffw", factors.getValue(friend, "fw"))
				factors.addValue(person, "ffp", factors.getValue(friend, "fp"))
				factors.addValue(person, "ffpt", factors.getValue(friend, "fpt"))

def getColumns(factors, columnNames):
	res = []
	for key in factors.values:
		factor = factors.values[key]
		values = []
		values.append(key)
		values.extend([factor.getValue(column) for column in columnNames])
		res.append(values)
	return res


def getFactorsForQuery(queryId, factors):

	queryFactorDict = {
		1: getColumns(factors, ["f", "ff"]),
		2: getColumns(factors, [ "f", "fp"]),
		3: getColumns(factors, ["ff", "ffp"]),
		4: getColumns(factors, ["fp", "f",  "fpt"]),
		5: getColumns(factors, ["ff", "ffg"]),	
		6: getColumns(factors, ["f","ff", "ffp", "ffpt"]),
		7: getColumns(factors, ["p", "pl"]),
		8: getColumns(factors, ["p", "pr"]), ### add "pr"
		9: getColumns(factors, ["ff", "ffp"]),
		10: getColumns(factors, ["ff", "ffp", "ffpt"]),
		11: getColumns(factors, ["ff", "ffw"]),
		12: getColumns(factors, ["f", "fp"]), ### add "fpr"
		13: getColumns(factors, ["ff"]),
		14: getColumns(factors, ["ff"])
	}

	return queryFactorDict[queryId]

def getCountryFactorsForQuery(queryId, factors):
	queryFactorDict = {
		3: getColumns(factors, ["p"]),
		11: getColumns(factors, ["p"]) ### replace with "org"
	}

	return queryFactorDict[queryId]

def getTagFactorsForQuery(queryId, factors):
	queryFactorDict = {
		6: getColumns(factors, ["p"]),
	}

	return queryFactorDict[queryId]

if __name__ == "__main__":
	argv = sys.argv
	if len(argv)< 3:
		print "arguments: <m0factors.txt> <m0friendsList.txt>"
		sys.exit(1)

	sys.exit(load(argv[1], argv[2]))


