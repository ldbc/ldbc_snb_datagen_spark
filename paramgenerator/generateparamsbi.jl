# class ParamsWriter:
#    def __init__(self, outdir, number, param_names):
#       self.file = codecs.open(outdir+"/bi_"+str(number)+"_param.txt", "w",encoding="utf-8")
#       for i in range(0,len(param_names)):
#          if i>0:
#             self.file.write("|")
#          self.file.write(param_names[i])
#       self.file.write("\n")

#    def append(self, params):
#       for i, param in enumerate(params):
#          if i>0:
#             self.file.write("|")
#          self.file.write(param)
#       self.file.write("\n")

# def key_params(sample, lower_bound, upper_bound):
#    results = []
#    for key, count in sample:
#       if count > lower_bound and count < upper_bound:
#          results.append([key, count])
#    return results

# def serialize_q6(outdir, tags):
#    writer = ParamsWriter(outdir, 6, ["tag"])
#    for tag, count in tags:
#       writer.append([tag])

#    # read precomputed counts from files
#    (personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givenNames,  ts, postsHisto) = \
#       readfactors.load(personFactorFiles,activityFactorFiles, friendsFiles)

#    tag_posts = tagFactors
#    tag_posts.sort(key=lambda x: x[1], reverse=True)

#    total_posts = 0
#    for day, count in tag_posts:
#       total_posts += count

#    serialize_q6 (outdir, key_params(tag_posts, total_posts/1300, total_posts/900))


## readfactors.py


# for inputFileName in activityFactorFiles:
#     with codecs.open(inputFileName, "r", "utf-8") as f:
#         countryCount = int(f.readline())
#         for i in range(countryCount):
#             line = f.readline().split(",")
#             country = line[0]
#             if not countries.existParam(country):
#                 countries.addNewParam(country)
#             countries.addValue(country, "p", int(line[1]))

#         tagClassCount = int(f.readline())
#         for i in range(tagClassCount):
#             line = f.readline().split(",")
#             tagClass = line[0]
#             if not tagClass in tagClasses:
#                 tagClasses[tagClass] = 0
#             tagClasses[tagClass] += int(line[2])

#         tagCount = int(f.readline())
#         for i in range(tagCount):
#             line = f.readline()
#             count = line[1+line.rfind(","):]
#             name = line[:line.rfind(",")]
#             if not name in tags:
#                 tags[name] = 0
#             tags[name] += int(count)

#         nameCount = int(f.readline())
#         for i in range(nameCount):
#             line = f.readline().split(",")
#             name = line[0]
#             if not name in names:
#                 names[name] = 0
#             names[name] += int(line[1])

#         for i in range(4):
#             t = f.readline().rstrip()
#             if timestamp[i] == 0 and t != 'null':
#                 timestamp[i] = int(t)

# loadFriends(friendFiles, results)

# return (results, countries, tags.items(), tagClasses.items(), names.items(), givenNames,timestamp, postsHisto)



##############################################################################
# Julia code starts from here
##############################################################################

# if length(ARGS) < 2
#     println("arguments: <input dir> <output dir>")
#     exit(1)
# end

# indir = ARGS[1] * "/"
# outdir = ARGS[2] * "/"

cd("/home/szarnyasg/git/ldbc/snb/ldbc_snb_datagen/paramgenerator")
indir = "../hadoop/"
outdir = "../substitution_out/"

files = readdir(indir)
activityFactorFiles = filter!(f -> endswith(f, "activityFactors.txt"), files)
#personFactorFiles = filter!(r"personFactors\.txt$", files)
#friendsFiles = filter!(r"^m0friendList", files)

println(activityFactorFiles)
#println(personFactorFiles)
#println(friendsFiles)

activityFactorFile = activityFactorFiles[1]
open(indir * activityFactorFile) do f
    countryCount = parse(Int64, readline(f))


    for i = 1:countryCount
        line = split(readline(f), ",")
        country = line[1]
        population = parse(Int64, line[2])
        println("$country: $population")
    end
end
