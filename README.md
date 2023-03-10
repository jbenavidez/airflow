# Airlow

Orchestration with Airflow.

## Description

This project contains multiples ETLs to handle different bussiness logic.

## Getting Started

### Dependencies

* Airflow
* Postgress
* Docker
* AWS


### Installing

* Install docker in your machine.


### Executing program

* Clone the repo
* On your terminal run: 
```
make init
```
* On your browser access to  http://localhost:8080/home
* Access to your container(Optional)
```
docker exec -it <container-id> /bin/bash
```

## login
On airlow Sign In page, use the following credentials
* username:airflow 
* password:airflow

![Alt text](/git_images/p2.png "test locally" )


## Set Postgres and AWS Creds on airflow UI
on airflow home page -> admin -> connection, click the "plus" to add new connection 

### Set Postgres connection values 

![Alt text](/git_images/p1.png "test locally" )

### Set AWS connection values 

![Alt text](/git_images/p3.png "test locally" )
 

## Test 

Test one or multiples Dag tasks using the following command:
```
airflow tasks test <dag_id>  <tastk_id> 2022-01-01
```


 
 