library(data.table)
library(igraph)
suppressMessages(require(bit64,quietly=TRUE,warn.conflicts=FALSE))
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

d2 <- df[,length(Comment.id),by=Person.id]
#message("STATISTICS: Comments/User || Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", round(mean(d2$V1)), " Median: ", round(median(d2$V1)))

message("\\hline  \\#comments/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")
