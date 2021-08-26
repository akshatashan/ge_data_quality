## data-etl

This repository contains data quality checks defined using great-expectations library for tables in bigquery.
In GCP, the airflow orchestration to run the dags is done via Cloud Composer. Cloud Composer is a manageed cloud solution for Airflow.

#### Local set up for cloud composer environment in gcp.
For the local setup, we use the official apache-airflow docker image. We chose the version of the image based on the version of python and airflow to match the corresponding versions on the Cloud composer environment.
Taking the official image, we build an extended image to install application related pip packages.
 ```
 docker build -t akshata/ge-data-quality -f Dockerfile .  && docker-compose up -d      
```

This will start up all containers required for airflow and airflow can be accessed via http://localhost:8080/
For a first time setup, create a user via the following command
```
 docker exec -it ge-data_airflow-webserver_1 airflow users create --username dev --password dev --role Admin --email xxx --firstname xxx --lastname xxx 
```
Login to airflow using `dev` as user name and password.

Since the volume for the dags folder is mapped, a change to the dags folder, does not need a rebuild of the image/container.
For dev, evironment variables are stored in `.env` file at the root of the project. In Prod, currently they are set via UI in the cloud composer environment. 

### Sync github dags folder to Composer environment 
After testing the dags in local environment, they can be uploaded to the composer environment via
```
    gcloud composer environments storage dags \ 
            import --environment ge-data
            --location europe-west1 
            --source <name of the dag here>
```
To install any application specific pip packages to the composer environment using the requirements file.
```
    gcloud composer environments update ge-data \
        --location europe-west1 \
        --update-pypi-packages-from-file requirements.txt
```
To add or update environmental variables to the composer environment
```
    gcloud composer environments update ge-data \
      --location europe-west1 \
      --update-env-variables=KEY=VALUE,KEY=VALUE
``` 


### Great Expectations library settings.
The prod and dev environments for the configuration of the great-expectations library is controlled by dags/great_expectations/great_expectations.yml file.
For dev, set the `config_variables_file_path` in this file to  `uncommitted/dev_config_variables.yml`. For prod, it is set to `uncommitted/config_variables.yml`
All validations and checkpoints for dev are configured to be stored in local machine. In prod, they are stored in GCP storage bucket, named `great-expectations`.

To interactively define expectations, use the jupyter notebook from `uncommitted/Create_Expectation_Suite.ipynb`

