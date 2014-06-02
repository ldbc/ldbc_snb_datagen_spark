library(data.table)
library(igraph)
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

#message("Set column names")
#names(df)=c("Post", "Person")
#setNames(df, c("Post", "Person"))

d2 <- df[,length(Post.id),by=Person.id]
message("STATISTICS: Posts/User || Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", mean(d2$V1), " Median: ", median(d2$V1))
