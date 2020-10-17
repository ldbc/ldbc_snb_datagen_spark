# DefaultDict comes from DataStructures #import Pkg; Pkg.add("DataStructures")
using DataStructures

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

countryFactors = Dict{String,Int64}()
tagClassFactors = DefaultDict{String,Int64}(0)
tagFactors = DefaultDict{String,Int64}(0)
nameFactors = DefaultDict{String,Int64}(0)
timestamps = Dict{String,Int64}()

activityFactorFile = activityFactorFiles[1]
open(indir * activityFactorFile) do f
    # read countryFactors
    # example: India,464151
    countryCount = parse(Int64, readline(f))
    for i = 1:countryCount
        line = split(readline(f), "|")
        country = line[1]
        population = parse(Int64, line[2])
        countryFactors[country] = population
    end

    # read tag classes
    # example: Thing,29737
    tagClassCount = parse(Int64, readline(f))
    for i = 1:tagClassCount
        line = split(readline(f), "|")
        tagClass = line[1]
        count = parse(Int64, line[2])
        tagClassFactors[tagClass] += count
    end

    # read tagFactors
    # example: Hamid_Karzai,8815
    # example: Frederick_III,_Holy_Roman_Emperor,19
    tagCount = parse(Int64, readline(f))
    for i = 1:tagCount
        line = split(readline(f), "|")
        tag = line[1]
        count = parse(Int64, line[2])
        tagFactors[tag] += + count
    end

    # read nameFactors
    # example: Daisuke,20
    nameCount = parse(Int64, readline(f))
    for i = 1:nameCount
        line = split(readline(f), "|")
        name = line[1]
        count = parse(Int64, line[2])
        nameFactors[name] += + count
    end

    # the last 4 lines are timestamps
    # instead of the Py code's array, we use a dictionary
    timestamps["startMonth"] = parse(Int64, readline(f))
    timestamps["startYear"] = parse(Int64, readline(f))
    timestamps["minWorkFrom"] = parse(Int64, readline(f))
    timestamps["maxWorkFrom"] = parse(Int64, readline(f))
end

countryFactors
tagClassFactors
tagFactors
nameFactors
timestamps

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

# function key_params(sample, lower_bound, upper_bound)
#     filter(e -> (lower_bound < e[2] && e[2] < upper_bound), sample)
# end

# bi6 = key_params(tag_posts, total_posts/1300, total_posts/900)
# bi6

tagClassFactors
tagFactors
nameFactors
timestamps
