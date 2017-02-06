#Load libraries
setwd('D:\\Sagar\\Large_rto\\R_Data\\rto_R_data\\model_build_data\\daily_rto_setup')
library(dplyr)
library(caret)
library(randomForest)

#Load existing rto model
load(file = "mymodel.rda")
ls()

#Load new csv raw data
ref_dat<-read.csv(file="model_build_data_4_months.csv",header = T,fileEncoding="UTF-8-BOM")

daily_data<-read.csv(file="export_data.csv",header = T,fileEncoding="UTF-8-BOM")
#daily_data <- read.csv(file="D:\\Sagar\\Large_rto\\R_Data\\rto_R_data\\model_build_data\\bbd_orders_data\\model_bbd_data.csv",header = T,fileEncoding="UTF-8-BOM")


#Format neccessary variables as factors 

#Match levels with old data
levels(daily_data$order_day) <- levels(ref_dat$order_day)
levels(daily_data$brand) <- levels(ref_dat$brand)
levels(daily_data$address_state) <- levels(ref_dat$address_state)
levels(daily_data$payment_type) <- levels(ref_dat$payment_type)
levels(daily_data$city_class) <- levels(ref_dat$city_class)

#Apply loaded model to new data
daily_data$rto_response <- predict(rf.rto ,daily_data,type='response')
daily_data$rto_prob<- predict(rf.rto ,daily_data,type='prob')

str(daily_data)

#Sort top 100 
pred_data <- daily_data[order(-as.numeric(daily_data$rto_response),-daily_data$rto_prob[,2]),]

date<-format(Sys.time(),"%Y%m%d_%H%M")

write.csv(pred_data,file=paste("./predicted_data/pred_data_",date,".csv",sep=""))
write.csv(pred_data,"pred_data.csv")
#write.csv(pred_data,"bbd_data.csv")

