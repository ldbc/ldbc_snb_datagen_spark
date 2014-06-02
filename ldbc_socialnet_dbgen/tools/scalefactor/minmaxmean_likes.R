library(data.table)
library(igraph)
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

message("Set column names")
#names(df)=c("Post", "Person")
setNames(df, c("Person", "PostOrCommont"))

d2 <- df[,length(PostOrCommont),by=Person]
message("STATISTICS: Likes/User || Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", mean(d2$V1), " Median: ", median(d2$V1))
