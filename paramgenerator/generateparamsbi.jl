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


function add_months(sourceYear, sourceMonth, sourceDay, nMonths)
    newMonth = sourceMonth - 1 + nMonths
    newYear = sourceYear + newMonth ÷ 12
    newMonth = newMonth % 12 + 1
    newDay = 1
    return (newYear, newMonth, newDay)
end


function convert_posts_histo(histogram, timestamps)
  week_posts = []
  for month in 1:length(histogram)
    # split total into 4 weeks
    monthTotal = histogram[month]
    (year, month, day) = add_months(2010, 01, 01, month-1)
    push!(week_posts, [(year, month, day   ) , monthTotal ÷ 4])
    push!(week_posts, [(year, month, day+7 ) , monthTotal ÷ 4])
    push!(week_posts, [(year, month, day+14) , monthTotal ÷ 4])
    push!(week_posts, [(year, month, day+21) , monthTotal ÷ 4])
  end

  return week_posts
end
week_posts = convert_posts_histo(postsHisto, timestamps)

function post_date_right_open_range_params(sample, lower_bound, upper_bound)
    results = []
    for ix in 1:length(sample)
        start_offset = sample[ix][1]
        count_sum = 0
        for v in sample[ix:length(sample)]
            count_sum += v[2]
        end
        if lower_bound < count_sum < upper_bound
            push!(results, [start_offset, count_sum])
        end
    end
    return results
end

function post_month_params(sample, lower_bound, upper_bound)
    results = []
    for ix in 0:length(sample)÷4 - 1
        start_ix = ix*4 + 1
        end_ix = min(ix*4 + 4, length(sample))
        count_sum = 0
        for v in sample[start_ix:end_ix]
            count_sum += v[2]
        end
        start_day = sample[start_ix][1]
        end_day = sample[min(end_ix + 1, length(sample))][1] # end_ix + 1 to get the first day of the next month
        if lower_bound < count_sum < upper_bound
            push!(results, [[start_day, end_day], count_sum])
        end
    end
    return results
end

persons = personFactors[:, :person]
shuffle!(MersenneTwister(1234), persons)

# sorting and sampling data

country_sample = countryFactors

tag_posts = tagFactors
tag_posts = sort(collect(tag_posts), by=x->x[2], rev=true)

tagclass_posts = tagClassFactors
tagclass_posts = sort(collect(tagclass_posts), by=x->x[2], rev=true)

total_posts = sum(values(tagFactors))
person_sum = sum(values(countryFactors))

#post_lower_threshold = 0.01*total_posts
#post_upper_threshold = 0.11*total_posts

week_posts = convert_posts_histo(postsHisto, timestamps)
non_empty_weeks = length(filter(x -> x[2] != 0, week_posts))

post_lower_threshold = (total_posts÷(non_empty_weeks÷4))*0.8
post_upper_threshold = (total_posts÷(non_empty_weeks÷4))*1.2

post_months = post_month_params(week_posts, post_lower_threshold, post_upper_threshold)
post_months

path_bounds = [2, 5] #TODO: this query needs revision anyways
language_codes = [["ar"], ["tk"], ["tk"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz", "tk"], ["uz", "tk"]]
post_lengths = [20,40,113,97,240]

# only keep values in (lower bound, upper bound) from sample
function key_params(sample, lower_bound, upper_bound)
    filter(e -> (lower_bound < e[2] && e[2] < upper_bound), sample)
end

# Q1 // 1
post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts)

# Q3 // 2
bi3 = post_months

# Q4 // 3
bi4 = []
for i in key_params(tagclass_posts, total_posts÷20, total_posts÷10)
    for j in key_params(country_sample, total_posts÷150, total_posts÷50)
        push!(bi4, [i[1], j[1]])
    end
end
bi4

# Q5 // 4
bi5 = key_params(country_sample, total_posts÷200, total_posts÷100)

# Q6 // 5
bi6 = key_params(tag_posts, total_posts÷1300, total_posts÷900)

# Q7 // 6
bi7 = key_params(tag_posts, total_posts÷900, total_posts÷600)

# Q8 // 7
bi8 = key_params(tag_posts, total_posts÷600, total_posts÷300)

# Q10 // 8
bi10 = []
for i in key_params(tag_posts, total_posts÷900, total_posts÷600)
    for j in post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts)
        push!(bi10, [i[1], j[1]])
    end
end
bi10

# Q14 // 9
post_months

# Q16 // 10
bi16 = []
r16 = MersenneTwister(1234)
for i in key_params(tagclass_posts, total_posts÷30, total_posts÷10)
    for j in key_params(country_sample, total_posts÷80, total_posts÷20)
        person = rand(r16, persons)
        push!(bi16, [i[1], j[1], person])
        # TODO: add bounds
    end
end
bi16

# Q17 // 11
bi17 = key_params(country_sample, total_posts÷200, total_posts÷100)

# Q18 // 12
bi18 = []
for week in post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts)
    for length in post_lengths
        for langs in language_codes
            push!(bi18, [week, length, langs])
        end
    end
end
bi18

# Q21 // 13
bi21 = []
for c in keys(key_params(country_sample, total_posts÷200, total_posts÷100))
    push!(bi21, [c, 1356994800000])
end
bi21

# Q22 // 14
bi22 = []
for i in keys(key_params(country_sample, total_posts÷120, total_posts÷40))
    for j in keys(key_params(country_sample, total_posts÷120, total_posts÷40))
        if i < j
            push!(bi22, [i, j])
        end
    end
end
bi22

# Q25 // 15
bi25 = []
r25 = MersenneTwister(1234)
for day_range in post_months
    for i in 1:min(length(persons), 10)
        person1Id = rand(r25, persons)
        person2Id = rand(r25, filter(p -> p != person1Id, persons))
        push!(bi25, [person1Id, person2Id, day_range[1], day_range[2]])
    end
end
bi25
