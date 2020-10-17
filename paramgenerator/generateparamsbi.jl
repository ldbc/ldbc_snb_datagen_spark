# class ParamsWriter:
#    def __init__(self, outdir, number, param_nameFactors):
#       self.file = codecs.open(outdir+"/bi_"+str(number)+"_param.txt", "w",encoding="utf-8")
#       for i in range(0,len(param_nameFactors)):
#          if i>0:
#             self.file.write("|")
#          self.file.write(param_nameFactors[i])
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

# def serialize_q6(outdir, tagFactors):
#    writer = ParamsWriter(outdir, 6, ["tag"])
#    for tag, count in tagFactors:
#       writer.append([tag])

#    # read precomputed counts from files
#    (personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givennameFactors,  ts, postsHisto) = \
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
#             if not countryFactors.existParam(country):
#                 countryFactors.addNewParam(country)
#             countryFactors.addValue(country, "p", int(line[1]))

#         tagClassCount = int(f.readline())
#         for i in range(tagClassCount):
#             line = f.readline().split(",")
#             tagClass = line[0]
#             if not tagClass in tagClassFactors:
#                 tagClassFactors[tagClass] = 0
#             tagClassFactors[tagClass] += int(line[2])

#         tagCount = int(f.readline())
#         for i in range(tagCount):
#             line = f.readline()
#             count = line[1+line.rfind(","):]
#             name = line[:line.rfind(",")]
#             if not name in tagFactors:
#                 tagFactors[name] = 0
#             tagFactors[name] += int(count)

#         nameCount = int(f.readline())
#         for i in range(nameCount):
#             line = f.readline().split(",")
#             name = line[0]
#             if not name in nameFactors:
#                 nameFactors[name] = 0
#             nameFactors[name] += int(line[1])

#         for i in range(4):
#             t = f.readline().rstrip()
#             if timestamp[i] == 0 and t != 'null':
#                 timestamp[i] = int(t)

# loadFriends(friendFiles, results)

# return (results, countryFactors, tagFactors.items(), tagClassFactors.items(), nameFactors.items(), givennameFactors,timestamp, postsHisto)



##############################################################################
# Julia code starts from here
##############################################################################

# if length(ARGS) < 2
#     println("arguments: <input dir> <output dir>")
#     exit(1)
# end

# indir = ARGS[1] * "/"
# outdir = ARGS[2] * "/"

cd("/home/szarnyasg/git/snb/ldbc_snb_datagen/paramgenerator")
indir = "../out/build/"
outdir = "../substitution_out/"

files = readdir(indir)
activityFactorFiles = filter(f -> endswith(f, "activityFactors.txt"), files)
#personFactorFiles = filter!(r"personFactors\.txt$", files)
#friendsFiles = filter!(r"^m0friendList", files)

println(activityFactorFiles)
#println(personFactorFiles)
#println(friendsFiles)

# DefaultDict
#import Pkg; Pkg.add("DataStructures")
using DataStructures

countryFactors = Dict{String,Int64}()
tagClassFactors = DefaultDict{String,Int64}(0)
tagFactors = DefaultDict{String,Int64}(0)
nameFactors = DefaultDict{String,Int64}(0)

activityFactorFile = activityFactorFiles[1]
open(indir * activityFactorFile) do f
    # read countryFactors
    # example: India,464151
    countryCount = parse(Int64, readline(f))
    for i = 1:countryCount
        line = split(readline(f), ",")
        country = line[1]
        population = parse(Int64, line[2])
        countryFactors[country] = population
    end

    # read tag classes
    # example: Thing,Thing,29737
    tagClassCount = parse(Int64, readline(f))
    for i = 1:tagClassCount
        line = split(readline(f), ",")
        tagClass = line[1]
        count = parse(Int64, line[3])
        tagClassFactors[tagClass] += count
    end

    # read tagFactors
    # example: Hamid_Karzai,8815
    # example: Frederick_III,_Holy_Roman_Emperor,19
    tagCount = parse(Int64, readline(f))
    for i = 1:tagCount
        line = split(readline(f), ",")
        tag = line[1]
        count = parse(Int64, line[end])
        tagFactors[tag] += + count
    end

    # read nameFactors
    # example: Daisuke,20
    nameCount = parse(Int64, readline(f))
    for i = 1:nameCount
        line = split(readline(f), ",")
        name = line[1]
        count = parse(Int64, line[2])
        nameFactors[name] += + count
    end

    # the last 4 lines are timestamps
    # TODO: copy the behaviour of the Py code
    # if timestamp[i] == 0 and t != 'null':
    #     timestamp[i] = int(t)
    for i = 1:4
        parse(Int64, readline(f))
    end
end

countryFactors
tagClassFactors
tagFactors
nameFactors

tag_posts = tagFactors
tag_posts = sort(collect(tag_posts), by=x->x[2], rev=true)

total_posts = 0
for (tag, count) in tag_posts
   global total_posts += count
end


#def key_params(sample, lower_bound, upper_bound):
#   results = []
#   for key, count in sample:
#      if count > lower_bound and count < upper_bound:
#         results.append([key, count])
#   return results

function key_params(sample, lower_bound, upper_bound)
    filter(e -> (lower_bound < e[2] && e[2] < upper_bound), sample)
end

bi6 = key_params(tag_posts, total_posts/1300, total_posts/900)
bi6
