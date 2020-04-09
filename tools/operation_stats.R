setwd("../") # shift to root
args <- commandArgs(trailingOnly = TRUE)
if (length(args)==0) {
  stop("Provide Scale Factor", call.=FALSE)
}
SF = args[1]
regen = as.logical(args[2])
cat("Dynamic entity operation counts for Scale Factor",SF,"\n")

if(regen==T) {
  cat("Regenerating data\n")
  system("./run.sh")
}


DATA_DIR = "./social_network/dynamic/"

# check for missing/incomplete data
entities = c("person","forum","post","comment","person_knows_person","forum_hasMember_person","person_likes_post","person_likes_comment")
generated = c()
for (i in 1:length(entities)) {
  generated[i] = as.logical(system(paste0("test -e ",DATA_DIR,entities[i],"_0_0.csv && echo T || echo F"), intern = TRUE))
}

if(sum(generated) < length(generated)) {
  cat("Missing or incomplete data, generating Scale Factor",SF,"\n")
  system(paste0("sed -i '' '1 s/^.*$/ldbc.snb.datagen.generator.scaleFactor:snb.interactive.",SF,"/' ./params.ini"))
  system("./run.sh")
} else {
  cat("Data exists\n")
}


# check matches SF requested
act_persons = system(paste0("wc -l ",DATA_DIR,"person_0_0.csv"), intern = TRUE)
act_persons = gsub(" ", "", act_persons, fixed = TRUE)
act_persons = as.numeric(gsub("\\..*","",act_persons))

SFS = c(0.1,0.3,1,10)
exp_persons = c(1700,3500,11000,27000,73000)


if(act_persons != exp_persons[which(SFS == SF)] + 1) {
  cat("Wrong Scale Factor, generating Scale Factor",SF,"\n")
  system(paste0("sed -i '' '1 s/^.*$/ldbc.snb.datagen.generator.scaleFactor:snb.interactive.",SF,"/' ./params.ini"))
  system("./run.sh")
} else {
  cat("Data correct Scale Factor\n")
}

# trim files
trimmed = c()
for (i in 1:length(entities)) {
  trimmed[i] = as.logical(system(paste0("test -e ",DATA_DIR,entities[i],"_0_0_dates.csv && echo T || echo F"), intern = TRUE))
}

if(sum(trimmed) != length(entities)) {
  cat("Preprocessing data files\n")
  for (i in 1:length(entities)) {
    if(i==2 | i==6) {
      system(paste0("cat ",DATA_DIR,entities[i],"_0_0.csv | cut -d'|' -f1-2,5 >> ",DATA_DIR,entities[i],"_0_0_dates.csv"))
    } else
       system(paste0("cat ",DATA_DIR,entities[i],"_0_0.csv | cut -d'|' -f1-2 >> ",DATA_DIR,entities[i],"_0_0_dates.csv"))
    }
  } else {
  cat("Data files preprocessed\n")
}


cat("Loading data files\n")

# read data
pers = read.csv2(paste0(DATA_DIR,"person_0_0_dates.csv"),sep = "|")
cat("1/8 person loaded\n")
frm = read.csv2(paste0(DATA_DIR,"forum_0_0_dates.csv"),sep = "|")
frm_w = frm[which(frm$type == "WALL"),]
frm_a = frm[which(frm$type == "ALBUM"),]
frm_g = frm[which(frm$type == "GROUP"),]
cat("2/8 forum loaded\n")
post = read.csv2(paste0(DATA_DIR,"post_0_0_dates.csv"),sep = "|")
cat("3/8 post loaded\n")
comm = read.csv2(paste0(DATA_DIR,"comment_0_0_dates.csv"),sep = "|")
cat("4/8 comment loaded\n")
knows = read.csv2(paste0(DATA_DIR,"person_knows_person_0_0_dates.csv"),sep = "|")
cat("5/8 knows loaded\n")
memb = read.csv2(paste0(DATA_DIR,"forum_hasMember_person_0_0_dates.csv"),sep = "|")
memb_w = memb[which(memb$type == "WALL"),]
memb_a = memb[which(memb$type == "ALBUM"),]
memb_g = memb[which(memb$type == "GROUP"),]
cat("6/8 memb loaded\n")
plikes = read.csv2(paste0(DATA_DIR,"person_likes_post_0_0_dates.csv"),sep = "|")
cat("7/8 post likes loaded\n")
clikes = read.csv2(paste0(DATA_DIR,"person_likes_comment_0_0_dates.csv"),sep = "|")
cat("8/8 comment likes loaded\n")




op_stats <- function(df) {

  c = as.Date(df$creationDate)
  c_count = sum(c < as.Date("2013-01-01"))
  d = as.Date(df$deletionDate)
  d_count = sum(d < as.Date("2013-01-01"))

  return(list(create=c_count,delete=d_count,perc=d_count/c_count))
}

create=c(op_stats(pers)$create,
         op_stats(frm_w)$create,op_stats(frm_a)$create,op_stats(frm_g)$create,
         op_stats(post)$create,op_stats(comm)$create,
         op_stats(knows)$create,
         op_stats(memb_w)$create,op_stats(memb_a)$create,op_stats(memb_g)$create,
         op_stats(plikes)$create,op_stats(clikes)$create)
delete=c(op_stats(pers)$delete,
         op_stats(frm_w)$delete,op_stats(frm_a)$delete,op_stats(frm_g)$delete,
         op_stats(post)$delete,op_stats(comm)$delete,
         op_stats(knows)$delete,
         op_stats(memb_w)$delete,op_stats(memb_a)$delete,op_stats(memb_g)$delete,
         op_stats(plikes)$delete,op_stats(clikes)$delete)
prop = delete/create
expected = c(0.07,0.00,0.01,0.01,0.005,0.005,0.05,0.00,0.05,0.05,0.01,0.01)
results = data.frame(SF=c('person','forum-walls','forum-album','forum-group','post','comment','knows',
                          'membership-walls','membership-album','membership-group','post likes','comment likes'),
                   create,
                    delete,
                     prop=round(prop,3),
                   exp = expected)
print(results)


