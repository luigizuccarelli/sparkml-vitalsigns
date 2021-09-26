# Spark Scala ML VitalSigns for scoring against NEWS chart

This is a Spark Machine Learning decision tree , written in Scala, 

# Credits

This project was cloned from https://github.com/caroljmcdonald/mapr-sparkml-churn, and modified for use with mleap

## Download
Download Spark with Hadoop from https://spark.apache.org/downloads.html
Select Spark 2.4.5 with Hadoop 2.7 (spark-2.4.5-bin-hadoop2.7.tgz)

## Data
The data files have purposely not been uploaded with this project. They are proprietary for now, until the ML and device/s have been approved.

## Launch with mleap
$bin/spark-shell  --packages ml.combust.mleap:mleap-spark_2.11:0.15.0,ml.combust.bundle:bundle-ml_2.11:0.15.0  --master local[1]

## Code
Clone the repo to your local machine

```
# change the variables to point to your local dev directory

# change directory to the docs/html folder
# remember to update the html code as well (variables)

cd docs/html 
val workdir = <your dev directory>
val bundledir = <your dev bundle directory>

# open the index.html page in any browser and follow the instructions
# we will be copying and pasting code (in blocks from the page to the scala shell)

```

Once we have created the bundle artifact we need to copy it to our RESTful API service

This is OpenShitf specifc, for kubectl there are various helper scripts to achieve this (search for kubectl rsync) 
```
# -v /tmp/models:/models
oc login url
oc rsync <bundledir> pod/mleap-xxx:/models
```

## Setup

Update the loadmodel-vitalsign.json file (update the name of your model)

Execute the following commands once the file has been copied

```
# commands
curl -v https://<url>/actuator/health
curl --header "Content-Type: application/json" --request POST --data "@loadmodeli-vitalsign.json" https:/url/models
curl --header "Content-Type: application/json" --request GET https:/url/models/<modelname>
curl --header "Content-Type: application/json" --header "timeout: 1000" --request POST --data "@lmz-leapframe-vitalsigns.json" https://url/models/<modelname>/transform

```

## Mleap spring-boot reference

https://github.com/combust/mleap/tree/master/mleap-spring-boot

