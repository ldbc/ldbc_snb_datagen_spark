# DefaultDict comes from DataStructures #import Pkg; Pkg.add("DataStructures")
using DataStructures
using CSV

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
personFactorFiles = filter(x -> occursin(r"personFactors\.txt$", x), files)
friendListFiles = filter(x -> occursin(r"friendList\d+\.csv$", x), files)

# factors

## activityFactors
countryFactors = Dict{String, Int64}()
tagClassFactors = DefaultDict{String, Int64}(0)
tagFactors = DefaultDict{String, Int64}(0)
nameFactors = DefaultDict{String, Int64}(0)
timestamps = Dict{String, Int64}()

activityFactorFile = activityFactorFiles[1]
open(indir * activityFactorFile) do f
    # read countryFactors
    # example: India|464151
    countryCount = parse(Int64, readline(f))
    for i = 1:countryCount
        line = split(readline(f), "|")
        country = line[1]
        population = parse(Int64, line[2])
        countryFactors[country] = population
    end

    # read tag classes
    # example: Thing|29737
    tagClassCount = parse(Int64, readline(f))
    for i = 1:tagClassCount
        line = split(readline(f), "|")
        tagClass = line[1]
        count = parse(Int64, line[2])
        tagClassFactors[tagClass] += count
    end

    # read tagFactors
    # example: Hamid_Karzai|8815
    # example: Frederick_III,_Holy_Roman_Emperor|19
    tagCount = parse(Int64, readline(f))
    for i = 1:tagCount
        line = split(readline(f), "|")
        tag = line[1]
        count = parse(Int64, line[2])
        tagFactors[tag] += count
    end

    # read nameFactors
    # example: Daisuke|20
    nameCount = parse(Int64, readline(f))
    for i = 1:nameCount
        line = split(readline(f), "|")
        name = line[1]
        count = parse(Int64, line[2])
        nameFactors[name] += count
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

## person factors
resultFactors = DefaultDict{Int64, DefaultDict{String, Int64}}(() -> DefaultDict{String, Int64}(0))
postsHisto = fill(0, 36+1) # TODO: make this configurable

personFactorFile = personFactorFiles[1]
personFactorRows = CSV.File(indir * personFactorFile; delim='|', header=["id", "name", "f", "p", "pl", "pt", "g", "w", "pr", "numMessages", "numForums"])

for row in personFactorRows
    global postsHisto
    resultFactors[row.id]["f"] = row.f
    resultFactors[row.id]["p"] += row.p
    resultFactors[row.id]["pl"] += row.pl
    resultFactors[row.id]["pt"] += row.pt
    resultFactors[row.id]["g"] += row.g
    resultFactors[row.id]["w"] += row.w
    resultFactors[row.id]["pr"] += row.pr
    numMessages = map(x -> parse(Int64, x), split(row.numMessages, ";"))
    postsHisto += numMessages
end

## friend list

personFriends = Dict{Int64,Array{Int64}}()

friendListFile = friendListFiles[1]
open(indir * friendListFile) do f
    # read countryFactors
    # examples:
    # 24189255811623|2199023256816|4398046512254|19791209300118|35184372089014
    # 30786325577800|1204|4398046511614
    # 30786325578463

    while (strline = readline(f)) != ""
        line = split(strline, "|")
        id = parse(Int64, line[1])
        friends = map(x -> parse(Int64, x), line[2:end])
        personFriends[id] = friends
    end

end

# using the list of friends, compute f* and ff* factors

for (person, friends) in personFriends
    resultFactors[person]["fp"] += sum(Int64[resultFactors[f]["p"] for f in friends])
end

for (person, friends) in personFriends
    resultFactors[person]["ffp"] += sum(Int64[resultFactors[f]["fp"] for f in friends])
end

resultFactors


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

summary(tagClassFactors)
summary(tagFactors)
summary(nameFactors)
summary(timestamps)
summary(personFriends)
summary(postsHisto)
