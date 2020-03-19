require(ggplot2)
require(ggthemes)

data = read.table("../src/main/resources/dictionaries/flashmobDist.txt")
x = as.numeric(data[,1])
y = (1:length(x)/length(x))
df = data.frame(x,y)

pdf(filename="flashmob_dist.pdf")
png(file="flashmob_dist.png")

ggplot(data=df,aes(x,y)) +
  geom_line() +
  labs(title="Empirical Flashmob Cumulative Distribution",x = "x", y="F(x)") +  
  theme_few()

dev.off()

