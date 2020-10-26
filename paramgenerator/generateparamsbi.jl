using DataStructures
using DataFrames
using CSV
using Random

# sample based on count,
# i.e. only keep values in (lower bound, upper bound) from sample
function key_params(df, lower_bound, upper_bound)
    df[lower_bound .< df.count .< upper_bound, :]
end

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

function add_months(sourceYear, sourceMonth, sourceDay, nMonths)
    newMonth = sourceMonth - 1 + nMonths
    newYear = sourceYear + newMonth ÷ 12
    newMonth = newMonth % 12 + 1
    newDay = 1
    return (newYear, newMonth, newDay)
end


function convert_posts_histo(histogram, timestamps) # TODO: use timestamps
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


# if length(ARGS) < 2
#     println("arguments: <input dir> <output dir>")
#     exit(1)
# end

# indir = ARGS[1] * "/"
# outdir = ARGS[2] * "/"

cd("../paramgenerator")
indir = "../out/build/"
outdir = "../substitution_out/"

###############################
### load CSVs #################
###############################

files = readdir(indir)

# activity factors
postsPerCountryFactorFiles = filter(f -> endswith(f, "postsPerCountryFactors.csv"), files)
tagClassCountFactorFiles = filter(f -> endswith(f, "tagClassCountFactors.csv"), files)
tagCountFactorFiles = filter(f -> endswith(f, "tagCountFactors.csv"), files)
firstNameCountFactorFiles = filter(f -> endswith(f, "firstNameCountFactors.csv"), files)
miscFactorFiles = filter(f -> endswith(f, "miscFactors.csv"), files)

# person factors
personFactorFiles = filter(x -> occursin(r"personFactors\.csv$", x), files)

# friends
friendsFiles = filter(x -> occursin(r"friendList\d+\.csv$", x), files)

# factors

## activityFactors
countFriends = CSV.read(indir * friendsFiles[1], DataFrame; delim='|', header=["person", "friendCount"])

country_sample = CSV.read(indir * postsPerCountryFactorFiles[1], DataFrame; delim='|', header=["country", "count"])
tagclass_posts = CSV.read(indir * tagClassCountFactorFiles[1], DataFrame; delim='|', header=["tagClass", "count"])
tag_posts = CSV.read(indir * tagCountFactorFiles[1], DataFrame; delim='|', header=["tag", "count"])
# unused in the BI workload
#nameFactors = CSV.read(indir * firstNameCountFactorFiles[1], DataFrame; delim='|', header=["name", "count"])
timestamps = CSV.read(indir * miscFactorFiles[1], DataFrame; delim='|')

## person, friend, and foaf factors
personFactorFile = personFactorFiles[1]
friendsFile = friendsFiles[1]

friends = CSV.read(indir * friendsFile, DataFrame; delim='|', header=["person", "friend"])
personFactors = CSV.read(indir * personFactorFile, DataFrame; delim='|', header=["person", "name", "f", "p", "pl", "pt", "g", "w", "pr", "numMessages", "numForums"])

###############################
### preprocess data frames ####
###############################

# create arrays from nested semicolon-separated fields
personFactors[!, :numMessages] = map.(x -> parse(Int64, x), split.(personFactors[!, :numMessages], ";"))
personFactors[!, :numForums] = map.(x -> parse(Int64, x), split.(personFactors[!, :numForums], ";"))
personFactorsAggregated = combine(
    groupby(personFactors, []),
    :numMessages => sum => :numMessages,
    :numForums => sum => :numForums # unused
)

# determine factors for friends by computing the sums of the factors grouped by person for the join expression:
# friends join_{friends[friend] = personFactors[person]} personFactors
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

# shuffling/ sorting and sampling data
persons = personFactors[:, :person]
shuffle!(MersenneTwister(1234), persons)
sort!(tag_posts, order(:count, rev=true))
sort!(tagclass_posts, order(:count, rev=true))

# aggregating data
total_posts = sum(tag_posts.count)
person_sum = sum(country_sample.count)

posts_histogram = personFactorsAggregated.numMessages[1]
week_posts = convert_posts_histo(posts_histogram, timestamps)
non_empty_weeks = length(filter(x -> x[2] != 0, week_posts))

# computing posts/month
post_lower_threshold = (total_posts÷(non_empty_weeks÷4))*0.8
post_upper_threshold = (total_posts÷(non_empty_weeks÷4))*1.2
post_months = post_month_params(week_posts, post_lower_threshold, post_upper_threshold)

# some constants
path_bounds = [2, 5] # TODO: path bound for BI Q16 // 10 need a revision anyways
language_codes = [["ar"], ["tk"], ["tk"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz"], ["uz", "tk"], ["uz", "tk"]]
post_lengths = [20,40,113,97,240]

###############################
### compute params for BI #####
###############################

# Q1 // 1
post_date_right_open_range_params(week_posts, 0.3*total_posts, 0.6*total_posts)

# Q3 // 2
bi3 = post_months

# Q4 // 3
bi4 = []
for i in eachrow(key_params(tagclass_posts, total_posts÷20, total_posts÷10))
    for j in eachrow(key_params(country_sample, total_posts÷150, total_posts÷50))
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
for i in eachrow(key_params(tag_posts, total_posts÷900, total_posts÷600))
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
for i in eachrow(key_params(tagclass_posts, total_posts÷30, total_posts÷10))
    for j in eachrow(key_params(country_sample, total_posts÷80, total_posts÷20))
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
for c in eachrow(key_params(country_sample, total_posts÷200, total_posts÷100))
    push!(bi21, [c[:country], 1356994800000])
end
bi21

# Q22 // 14
bi22 = []
for i in eachrow(key_params(country_sample, total_posts÷120, total_posts÷40))
    for j in eachrow(key_params(country_sample, total_posts÷120, total_posts÷40))
        if i[:country] < j[:country]
            push!(bi22, [i[:country], j[:country]])
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
