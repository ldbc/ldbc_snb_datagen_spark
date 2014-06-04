library(data.table)
library(igraph)
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2)
df <- rbindlist(dflist)

#message("Set column names")
#names(df)=c("Post", "Person")
#setNames(df, c("Post", "Person"))

d2 <- df[,length(Post.id),by=Person.id]
#message("STATISTICS: Posts/User || Min: ",min(d2$V1),", Max: ", max(d2$V1), " Mean: ", round(mean(d2$V1)), " Median: ", round(median(d2$V1)))

message("\\hline  \\#posts/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")

message("Plot histogram #post/users")
pdf("numPostsUserHist.pdf")
hist(d2$V1,main="Histogram #posts per user", xlab="Number of posts", ylab="Number of users")	
dev.off()
