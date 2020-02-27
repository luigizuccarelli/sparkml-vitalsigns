# Spark Scala ML Churn

This is an example using Spark Machine Learning decision trees , written in Scala, 

## Download
Download Spark with Hadoop from https://spark.apache.org/downloads.html
Select Spark 2.4.5 with Hadoop 2.7 (spark-2.4.5-bin-hadoop2.7.tgz)

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

```
# -v /tmp/models:/models
oc login url
oc rsync <bundledir> pod/mleap-xxx:/models
```

## Setup

Update the loadmodel.json file (update the name of your model)

Execute the following commands once the file has been copied

```
# openshift online
curl -v https://mleap-portfoliotracker.e4ff.pro-eu-west-1.openshiftapps.com/actuator/health

curl --header "Content-Type: application/json" --request POST --data "@loadmodel.json" https:/url/models
curl --header "Content-Type: application/json" --request POST --data "@loadmodel.json" https://mleap-portfoliotracker.e4ff.pro-eu-west-1.openshiftapps.com/models

curl --header "Content-Type: application/json" --request GET https:/url/models/<modelname>
curl --header "Content-Type: application/json" --request GET https://mleap-portfoliotracker.e4ff.pro-eu-west-1.openshiftapps.com/models/churn-mleap

curl --header "Content-Type: application/json" --header "timeout: 1000" --request POST --data "@example-leapframe.json" https://url/models/<modelname>/transform
curl --header "Content-Type: application/json" --header "timeout: 1000" --request POST --data "@example-leapframe.json" https://mleap-portfoliotracker.e4ff.pro-eu-west-1.openshiftapps.com/models/<modelname>/transform


```


