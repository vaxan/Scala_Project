# Scala_Project
The project focuses on analyzing accidents from the past 10 years which occurred in the UK. The dataset has been collected from online data science portal Kaggle URL(https://www.kaggle.com/benoit72/uk-accidents-10-years-history-with-many-variables).

Data Description:
Accident: main data set contains information about accident severity, weather, location, date, hour, day of the week, road type, etc.
Vehicle: contains information about vehicle type, vehicle model, engine size, driver sex, driver age, car age, etc.
Casualties: contains information about casualty severity, age, sex social class, casualty type, pedestrian or car passenger, etc.
Lookup: contains the text description of all variable code in the three files. Each file contains about 1640597 values.

As part of the data analysis project tries to answer the below questions.
1. Does weather have any influence on accidents?

2.Most casualties when an accident occurs?
Casualty refers to the person(passenger, driver, etc.) who are injured from the accident it can be (fatal, serious, slight)

3.Time series analysis of accident(Year/Month/Time).
To be able to predict who is responsible for the accident given some conditions using Spark ML algorithm Random Forest.


 Random Forest Classifier

To be able to predict who is responsible for the accident given some conditions using Spark ML algorithm Random Forest.

In order to predict casualty class that is the person responsible for the accident which is classified into 3 classes those are Driver, Passenger, Pedestrian I have used random forest classifier. Using this model I was able to achieve an accuracy of 0.89(89%).


Visualization of the analysis performed on the raised question in the readme description part. Visualization is done using Python Seaborn library.
