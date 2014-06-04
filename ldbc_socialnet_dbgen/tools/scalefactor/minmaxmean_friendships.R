library(data.table)
library(igraph)
suppressMessages(require(bit64,quietly=TRUE,warn.conflicts=FALSE))
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

message("Set column names")
#names(df)=c("Post", "Person")
setNames(df, c("Person1", "Person2"))

d2 <- df[,length(Person2),by=Person1]
#message("STATISTICS: Friendships/User || Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", mean(d2$V1), " Median: ", median(d2$V1))

message("\\hline  \\#friends/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")
