library(data.table)
library(igraph)
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

d2 <- df[,length(Comment.id),by=Person.id]
message("Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", mean(d2$V1), " Median: ", median(d2$V1))
