Basic code guidelines [2013-07-24]

ldbc.socialnet.dbgen.generator: The main directory. Contains the program entry class MRGenerateUsers which uses hadoop 
jobs and our class "ScalableGenerator" to generate all the data.
It also contains any supportive classes needed to generate the data.

ldbc.socialnet.dbgen.dictionary: Contains the classes responsible of reading the file datasets from the dictionaries 
folder and provides methods to access such data.

ldbc.socialnet.dbgen.objects: The schema entities classes are in this folder.

ldbc.socialnet.dbgen.serializer: The generator serializers.

ldbc.socialnet.dbgen.vocabulary: RDF vocabulary classes used in the serializers. 

ldbc.socialnet.dbgen.util: Any additional classes which doesn't belong in any other directory.