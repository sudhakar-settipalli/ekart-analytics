# Working Directory
setwd('D:\\Sagar\\Large_rto\\R_Data\\rto_R_data\\model_build_data')
# Loading the required packages and custome Functions
#install.packages("dplyr")
library('dplyr')
#install.packages('polycor')
#library(Hmisc)
library(polycor)
#install.packages('usdm')
library(usdm)
#install.packages('lift')
#install.packages("ROCR", dependencies=TRUE)
library(e1071)
library(caret)
library(lattice)
library(ggplot2)
library(ROCR)
library(lift)

#install.packages("gains")
library(gains)
# Load library
#install.packages("randomForest")
library(randomForest)
# Help on ramdonForest package and function
#library(help=randomForest)
#help(randomForest)

## Read data
termCrosssell<-read.csv(file="model_data.csv",header = T,fileEncoding="UTF-8-BOM")
test_data<-read.csv(file="model_data.csv",header = T,fileEncoding="UTF-8-BOM")
val_test<-read.csv(file="val_data.csv",header = T,fileEncoding="UTF-8-BOM")



bbd_order_data <-read.csv(file= "D:/Sagar/Large_rto/R_Data/rto_R_data/model_build_data/bbd_orders_data/model_bbd_data.csv",header = T,fileEncoding="UTF-8-BOM")

levels(bbd_order_data$address_state) <- levels(cross.sell.dev$address_state)


#termCrosssell<-bbd_order_data
test_data<-termCrosssell


## Explore data frame

termCrosssell[,names] <- lapply(termCrosssell[,names] , factor)
#termCrosssell <- termCrosssell[,!names(termCrosssell) %in% 'is_rto'] 
test_data[,names] <- lapply(test_data[,names] , factor)

#test_data <- test_data[,!names(termCrosssell) %in% 'is_rto']
str(termCrosssell[,varNames_gini])
str(test_data[,varNames_gini])

table(termCrosssell$is_rto)/nrow(termCrosssell)
table(test_data$is_rto)/nrow(test_data)
#0         1 
#0.8928807 0.1071193

sample.ind <- sample(2, 
                     nrow(termCrosssell),
                     replace = T,
                     prob = c(0.7,0.3))
cross.sell.dev <- termCrosssell[sample.ind==1,]
cross.sell.val <- termCrosssell[sample.ind==2,]

table(cross.sell.dev$is_rto)/nrow(cross.sell.dev)

table(cross.sell.val$is_rto)/nrow(cross.sell.val)
class(cross.sell.dev$is_rto)
class(test_data$is_rto)

#Make Formula
varNames <- names(cross.sell.dev)

varNames_gini <- varNames[varNames %in% c('customer_contacts_per_order','order_day','brand','address_state','customer_recency_in_days','customer_rto_percent','final_promised_sla_in_days','product_contacts_per_order','product_frequency_in_days','order_original_billing_amount_final','product_rto_percent','product_rvp_percent','payment_type','city_class','promotion_discount_actual')]

varNames_form <- paste(varNames_gini, collapse = "+")


# Add response variable and convert to a formula object
rf.form <- as.formula(paste("is_rto", varNames_form, sep = " ~ "))

#Building Random Forest using R
rf.rto <- randomForest(rf.form,
                       cross.sell.dev,
                       ntree=501,
                       importance=T,
                       mtry=3)

cross.sell.rf <- rf.rto

#predict(cross.sell.rf1 ,cross.sell.dev,type='response')

# Create Confusion Matrix
confusionMatrix(data=cross.sell.dev$predicted.response,
                reference=cross.sell.dev$is_rto,
                positive='1')

# Predicting response variable validation data
cross.sell.val$predicted.response <- predict(cross.sell.rf ,cross.sell.val)
cross.sell.val$predicted.prob<- predict(cross.sell.rf ,cross.sell.val,type='prob')


# Create Confusion Matrix
confusionMatrix(data=cross.sell.val$predicted.response,
                reference=cross.sell.val$is_rto,
                positive='1')

# Predicting response variable test data
test_data$predicted.response <- predict(cross.sell.rf ,test_data)
test_data$predicted.prob <-  predict(cross.sell.rf ,test_data,type='prob', index=1)

bbd_order_data$predicted.response2 <- predict(cross.sell.rf ,bbd_order_data)
bbd_order_data$predicted.prob2 <- predict(cross.sell.rf ,bbd_order_data,type='prob', index=1)

predict(cross.sell.rf ,bbd_order_data,type='prob', index=1)

levels(bbd_order_data$payment_type) <- levels(test_data$payment_type)

write.csv(bbd_order_data,"bbd_rto.csv")


str(bbd_order_data[,varNames_gini])

# Create Confusion Matrix
confusionMatrix(data=test_data$predicted.response,
                reference=test_data$is_rto,
                positive='1')


z<-rf.rto$confusion
write.csv(z,"accu_matrix_table.csv")
F_measure=double(10)
Recall=double(10)
Precision=double(10)
