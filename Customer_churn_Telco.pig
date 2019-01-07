//Problem 1:Display average bill (Total charges) paid by male and female.

telco_data = LOAD 'customer_churn.csv' using PigStorage(',') AS (customerID:chararray,gender:chararray,SeniorCitizen:int,Partner:chararray,Dependents:chararray,tenure:int,PhoneService:chararray,MultipleLines:chararray,	InternetService:chararray,OnlineSecurity:chararray,OnlineBackup:chararray,DeviceProtection:chararray,TechSupport:chararray,StreamingTV:chararray,StreamingMovies:chararray,Contract:chararray,PaperlessBilling:chararray,PaymentMethod:chararray,MonthlyCharges:float,TotalCharges:float,Churn:chararray)
dataset_1 = FOREACH telco_data GENERATE gender,TotalCharges;
filter_M_F = FILTER dataset_1 BY gender is not null;
grp_M_F = GROUP dataset_1 BY gender;
avg_M_F = FOREACH grp_M_F GENERATE group, AVG(dataset_1.TotalCharges);


//Problem 2: Display the most frequently used payment method a male.
telco_data = LOAD 'customer_churn.csv' using PigStorage(',') AS (customerID:chararray,gender:chararray,SeniorCitizen:int,Partner:chararray,Dependents:chararray,tenure:int,PhoneService:chararray,MultipleLines:chararray,	InternetService:chararray,OnlineSecurity:chararray,OnlineBackup:chararray,DeviceProtection:chararray,TechSupport:chararray,StreamingTV:chararray,StreamingMovies:chararray,Contract:chararray,PaperlessBilling:chararray,PaymentMethod:chararray,MonthlyCharges:float,TotalCharges:float,Churn:chararray)
dataset_2 = FOREACH telco_data GENERATE customerID,gender,PaymentMethod;
filter_M = FILTER dataset_2 BY gender=='Male';
grp_payment_mthd = GROUP filter_M BY PaymentMethod;
count_by_payment_mthd = FOREACH grp_payment_mthd GENERATE group,COUNT(filter_M.customerID);
ord_by_pay_mthd = ORDER count_by_payment_mthd BY $1 DESC;
Most_used_by_M = LIMIT ord_by_pay_mthd 1;


//Problem 3:If the customer is not having any internet connection then what is the minimum bill that the customer has paid.
telco_data = LOAD 'customer_churn.csv' using PigStorage(',') AS (customerID:chararray,gender:chararray,SeniorCitizen:int,Partner:chararray,Dependents:chararray,tenure:int,PhoneService:chararray,MultipleLines:chararray,	InternetService:chararray,OnlineSecurity:chararray,OnlineBackup:chararray,DeviceProtection:chararray,TechSupport:chararray,StreamingTV:chararray,StreamingMovies:chararray,Contract:chararray,PaperlessBilling:chararray,PaymentMethod:chararray,MonthlyCharges:float,TotalCharges:float,Churn:chararray)
dataset_3 = FOREACH telco_data GENERATE customerID,InternetService,TotalCharges;
Filter_No_IS = FILTER dataset_3 BY InternetService=='No';
grp_Min_bill = GROUP Filter_No_IS ALL;
Min_Bill = FOREACH grp_Min_bill GENERATE MIN(TotalCharges);


//Problem 4:Count the number of senior citizen who have dependents (i.e. 1) and give them 20% discount on final bill and marked them as 'Sr Discount'.
telco_data = LOAD 'customer_churn.csv' using PigStorage(',') AS (customerID:chararray,gender:chararray,SeniorCitizen:int,Partner:chararray,Dependents:chararray,tenure:int,PhoneService:chararray,MultipleLines:chararray,	InternetService:chararray,OnlineSecurity:chararray,OnlineBackup:chararray,DeviceProtection:chararray,TechSupport:chararray,StreamingTV:chararray,StreamingMovies:chararray,Contract:chararray,PaperlessBilling:chararray,PaymentMethod:chararray,MonthlyCharges:float,TotalCharges:float,Churn:chararray)
dataset_4 = FOREACH telco_data GENERATE customerID,SeniorCitizen,Dependents,TotalCharges;
filter_sr = FILTER dataset_4 BY (SeniorCitizen==1) AND (Dependents=='Yes');
cnt_sr_dep_1 = FOREACH (GROUP filter_sr ALL) GENERATE COUNT(filter_sr.customerID);
sr_discount = FOREACH filter_sr GENERATE customerID, (TotalCharges-(.20*TotalCharges)) Discounted_bill;

//Problem 5:Display customer id and their tenure whose tenure is more than 20 years and are using paperless billing method to save an environment
telco_data = LOAD 'customer_churn.csv' using PigStorage(',') AS (customerID:chararray,gender:chararray,SeniorCitizen:int,Partner:chararray,Dependents:chararray,tenure:int,PhoneService:chararray,MultipleLines:chararray,	InternetService:chararray,OnlineSecurity:chararray,OnlineBackup:chararray,DeviceProtection:chararray,TechSupport:chararray,StreamingTV:chararray,StreamingMovies:chararray,Contract:chararray,PaperlessBilling:chararray,PaymentMethod:chararray,MonthlyCharges:float,TotalCharges:float,Churn:chararray)
dataset_5 = FOREACH telco_data GENERATE customerID,tenure,PaperlessBilling;
Filter_paper_tenure = FILTER dataset_5 BY (tenure>20) AND (PaperlessBilling=='Yes');
cust_id_more_than_20 = FOREACH Filter_paper_tenure GENERATE customerID,tenure;
dump cust_id_more_than_20;


//Problem 6:Which is the most preferred internet service used by the customer, is it for Fibre optic or for DSL?
telco_data = LOAD 'customer_churn.csv' using PigStorage(',') AS (customerID:chararray,gender:chararray,SeniorCitizen:int,Partner:chararray,Dependents:chararray,tenure:int,PhoneService:chararray,MultipleLines:chararray,	InternetService:chararray,OnlineSecurity:chararray,OnlineBackup:chararray,DeviceProtection:chararray,TechSupport:chararray,StreamingTV:chararray,StreamingMovies:chararray,Contract:chararray,PaperlessBilling:chararray,PaymentMethod:chararray,MonthlyCharges:float,TotalCharges:float,Churn:chararray);
dataset_6 = FOREACH telco_data GENERATE customerID,InternetService;
Filter_data = FILTER dataset_6 BY InternetService != 'No';
grp_data = GROUP Filter_data BY InternetService;
cnt_int_service = FOREACH grp_data GENERATE group,COUNT(Filter_data.InternetService);
sort_int_service = ORDER cnt_int_service BY $1 DESC;
lmt_int_service = limit sort_int_service 1;
dump lmt_int_service;


//Problem 7:Customer who are using Streaming movies option. Calculate its final bill by increasing the monthly bill by 9.5%
telco_data = LOAD 'customer_churn.csv' using PigStorage(',') AS (customerID:chararray,gender:chararray,SeniorCitizen:int,Partner:chararray,Dependents:chararray,tenure:int,PhoneService:chararray,MultipleLines:chararray,	InternetService:chararray,OnlineSecurity:chararray,OnlineBackup:chararray,DeviceProtection:chararray,TechSupport:chararray,StreamingTV:chararray,StreamingMovies:chararray,Contract:chararray,PaperlessBilling:chararray,PaymentMethod:chararray,MonthlyCharges:float,TotalCharges:float,Churn:chararray);
dataset_7 = FOREACH telco_data GENERATE customerID,StreamingMovies,TotalCharges;
Filter_data = FILTER dataset_7 BY StreamingMovies=='Yes';
Final_bill = FOREACH Filter_data GENERATE customerID, (TotalCharges+TotalCharges*0.095) AS final_bill;
Dump Final_bill;
