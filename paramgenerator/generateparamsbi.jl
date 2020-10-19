using DataStructures
using DataFrames
using CSV
using Random

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
friendsFiles = filter(x -> occursin(r"friendList\d+\.csv$", x), files)

# factors

# TODO: use data frames to parse these

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

## person, friend, and foaf factors
personFactorFile = personFactorFiles[1]
friendsFile = friendsFiles[1]

friends = CSV.read(indir * friendsFile, DataFrame; delim='|', header=["person", "friend"])
personFactors = CSV.read(indir * personFactorFile, DataFrame; delim='|', header=["person", "name", "f", "p", "pl", "pt", "g", "w", "pr", "numMessages", "numForums"])

personFactors[!, :numMessages] = map.(x -> parse(Int64, x), split.(personFactors[!, :numMessages], ";"))
personFactors[!, :numForums] = map.(x -> parse(Int64, x), split.(personFactors[!, :numForums], ";"))

personFactorsAggregated = combine(
    groupby(personFactors, []),
    :numMessages => sum => :numMessages,
    :numForums => sum => :numForums
)

postsHisto = personFactorsAggregated.numMessages[1]

# determine factors for friends by computing the sums of the factors grouped by person for friends join_{friends[friend] = personFactors[person]} personFactors
friendsFactors = 
    combine(
        groupby(
            innerjoin(friends, personFactors, on = [:friend => :person])
            , :person
        ),
        :f => sum => :f,
        :p => sum => :p,
        :pl => sum => :pl,
        :pt => sum => :pt,
        :g => sum => :g,
        :w => sum => :w,
        :pr => sum => :pr
    )

# determine factors for friends of friends
foafFactors =
    combine(
        groupby(
            innerjoin(friends, friendsFactors, on = [:friend => :person])
            , :person
        ),
        :f => sum => :f,
        :p => sum => :p,
        :pl => sum => :pl,
        :pt => sum => :pt,
        :g => sum => :g,
        :w => sum => :w,
        :pr => sum => :pr
    )

summary(tagClassFactors)
summary(tagFactors)
summary(nameFactors)
summary(timestamps)
summary(postsHisto)

# def add_months(sourcedate,months):
#    month = sourcedate.month - 1 + months
#    year = int(sourcedate.year + month // 12 )
#    month = month % 12 + 1
#    day = 1
#    return sourcedate.replace(year, month, day)


function add_months(year, month, day, months)
    baseMonth = month - 1 + months
    baseYear = year + month ÷ 12
    baseMonth = baseMonth % 12 + 1
    baseDay = 1
    return (baseYear, baseMonth, baseDay)
end


function convert_posts_histo(histogram, timestamps)
  week_posts = []
  for month in 1:length(histogram)
    # split total into 4 weeks
    monthTotal = histogram[month]
    (baseYear, baseMonth, baseDay) = add_months(2010, 01, 01, month)
    append!(week_posts, [(baseYear, baseMonth, baseDay    , monthTotal ÷ 4)])
    append!(week_posts, [(baseYear, baseMonth, baseDay+7  , monthTotal ÷ 4)])
    append!(week_posts, [(baseYear, baseMonth, baseDay+14 , monthTotal ÷ 4)])
    append!(week_posts, [(baseYear, baseMonth, baseDay+21 , monthTotal ÷ 4)])
  end

  return week_posts
end

week_posts = convert_posts_histo(postsHisto, timestamps)
non_empty_weeks = length(filter(x -> x[4] != 0, week_posts))

non_empty_weeks=len(week_posts)
for ix in range(0,len(week_posts)):
    if week_posts[ix][1]==0:
        non_empty_weeks-= 1
print(non_empty_weeks)

persons = personFactors[:, :person]
shuffle!(MersenneTwister(1234), persons)

# sorting and sampling data

country_sample = countryFactors

tag_posts = tagFactors
tag_posts = sort(collect(tag_posts), by=x->x[2], rev=true)

tagclass_posts = tagClassFactors
tagclass_posts = sort(collect(tagclass_posts), by=x->x[2], rev=true)

total_posts = sum(values(tagFactors))
person_sum = sum(TODO)

post_lower_threshold = 0.01*total_posts
post_upper_threshold = 0.11*total_posts

# TODO: why are these redefined?
#post_lower_threshold = (total_posts÷(non_empty_weeks÷4))*0.8
#post_upper_threshold = (total_posts÷(non_empty_weeks÷4))*1.2


# non_empty_weeks=len(week_posts)
# for ix in range(0,len(week_posts)):
#    if week_posts[ix][1]==0:
#       non_empty_weeks-= 1


# only keep values in (lower bound, upper bound) from sample
function key_params(sample, lower_bound, upper_bound)
    filter(e -> (lower_bound < e[2] && e[2] < upper_bound), sample)
end

#bi4 = key_params(tagclass_posts, total_posts÷20, total_posts÷10)
#key_params(country_sample, total_posts÷20, total_posts÷10)
#bi5 = key_params(tag_posts, total_posts÷150, total_posts÷50)

bi6 = key_params(tag_posts, total_posts÷1300, total_posts÷900)
bi7 = key_params(tag_posts, total_posts÷900, total_posts÷600)
bi8 = key_params(tag_posts, total_posts÷600, total_posts÷300)
