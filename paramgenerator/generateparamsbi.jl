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
activityFactorFiles = filter!(r"activityFactors\.txt$", files)
personFactorFiles = filter!(r"personFactors\.txt$", files)
friendsFiles = filter!(r"^m0friendList", files)

println(activityFactorFiles)
println(personFactorFiles)
println(friendsFiles)

#    # read precomputed counts from files
#    (personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givenNames,  ts, postsHisto) = \
#       readfactors.load(personFactorFiles,activityFactorFiles, friendsFiles)

#    tag_posts = tagFactors
#    tag_posts.sort(key=lambda x: x[1], reverse=True)

#    total_posts = 0
#    for day, count in tag_posts:
#       total_posts += count

#    serialize_q6 (outdir, key_params(tag_posts, total_posts/1300, total_posts/900))
