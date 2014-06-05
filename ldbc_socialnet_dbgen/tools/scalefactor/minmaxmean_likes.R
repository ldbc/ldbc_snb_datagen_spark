library(data.table)
library(igraph)
suppressMessages(require(bit64,quietly=TRUE,warn.conflicts=FALSE))
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

message("Set column names")
#names(df)=c("Post", "Person")
setNames(df, c("Person", "PostOrCommont"))

d2 <- df[,length(PostOrCommont),by=Person]

#message("STATISTICS: Likes/User || Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", mean(d2$V1), " Median: ", median(d2$V1))

message("\\hline  \\#likes/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")
