
args <- commandArgs(trailingOnly=TRUE);
print(args[1]);
data <- as.matrix(read.table(paste(args[1],sep=''),header=T))
n = sum(!is.na(data))
data.ecdf = ecdf(data)
data.mean = mean(data)
data.max = max(data)
data.min = min(data)

postscript(args[2])
plot(data.ecdf, xlim=c(0.5,data.max), log="x", xlab = args[3], ylab = "cumulative probability", main = '')
text((data.max - data.min)/10,0.9,paste("Average degree: ",data.mean,sep=" "))
dev.off()
