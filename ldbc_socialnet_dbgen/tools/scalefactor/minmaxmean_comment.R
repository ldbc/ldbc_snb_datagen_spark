library(data.table)
library(igraph)
require(bit64)	
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

library(plyr)
d2 <- ddply(df, "Person.id", summarize, PCount = length(Comment.id))
d3 <- as.numeric(d2$PCount)
message("\\hline  \\#comments/user  &", min(d3) ,  " &  ", max(d3),  " & ", round(mean(d3)) , " & ", round(median(d3)), " \\\\")

#d2 <- df[,length(Comment.id),by=Person.id]
#message("\\hline  \\#comments/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")

message("Plot histogram #comment/users")
pdf("numCommentsUserHist.pdf")
hist(d3,main="Histogram #comments per user", xlab="Number of comments", ylab="Number of users")	
dev.off()
