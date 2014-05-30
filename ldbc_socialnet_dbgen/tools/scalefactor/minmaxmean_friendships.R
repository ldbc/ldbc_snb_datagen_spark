library(data.table)
library(igraph)
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

message("Set column names")
#names(df)=c("Post", "Person")
setNames(df, c("Person1", "Person2"))

d2 <- df[,length(Person2),by=Person1]
message("Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", mean(d2$V1), " Median: ", median(d2$V1))
