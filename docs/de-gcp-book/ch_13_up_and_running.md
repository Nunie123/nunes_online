# Up and Running: Data Engineering on the Google Cloud Platform
The completely free E-Book for setting up and running a Data Engineering stack on Google Cloud Platform.

NOTE: This book is currently incomplete. If you find errors or would like to fill in the gaps, read the [Contributions section](https://github.com/Nunie123/data_engineering_on_gcp_book#user-content-contributions).

## Table of Contents
[Preface](https://github.com/Nunie123/data_engineering_on_gcp_book) <br>
[Chapter 1: Setting up a GCP Account](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_01_gcp_account.md) <br>
[Chapter 2: Setting up Batch Processing Orchestration with Composer and Airflow](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_02_orchestration.md) <br>
[Chapter 3: Building a Data Lake with Google Cloud Storage (GCS)](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_03_data_lake.md) <br>
[Chapter 4: Building a Data Warehouse with BigQuery](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_04_data_warehouse.md) <br>
[Chapter 5: Setting up DAGs in Composer and Airflow](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_05_dags.md) <br>
[Chapter 6: Setting up Event-Triggered Pipelines with Cloud Functions](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_06_event_triggers.md) <br>
[Chapter 7: Parallel Processing with Dataproc and Spark](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_07_parallel_processing.md) <br>
[Chapter 8: Streaming Data with Pub/Sub](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_08_streaming.md) <br>
[Chapter 9: Managing Credentials with Google Secret Manager](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_09_secrets.md) <br>
[Chapter 10: Infrastructure as Code with Terraform](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_10_infrastructure_as_code.md) <br>
[Chapter 11: Deployment Pipelines with Cloud Build](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_11_deployment_pipelines.md) <br>
[Chapter 12: Monitoring and Alerting](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_12_monitoring.md) <br>
**Chapter 13: Up and Running - Building a Complete Data Engineering Infrastructure** <br>
[Appendix A: Example Code Repository](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/appendix_a_example_code/README.md)


---

# [Chapter 13](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_13_up_and_running.md): Up and Running - Building a Complete Data Engineering Infrastructure

In the previous 12 chapters you learned: 
* how to set up batch and streaming data pipelines
* how to orchestrate your pipelines
* how to transform your data
* how to set up your data warehouse
* how to deploy your code
* how to set up infrastructure as code
* how to monitor your infrastructure

In this chapter we are going to cover all of that in one go: setting up a complete data engineering infrastructure for a fictional company. I'm not going to go into as much detail for each piece we are going to set up, as we already covered the details in previous chapters. Instead, this chapter will walk you through the steps you would take if you needed to get a complete infrastructure up and running on GCP.

I mentioned this in the introduction, but I'll repeat it here: what is covered in this book is not **THE** data engineering stack, it is **A** data engineering stack. Even within GCP there are lots of ways to accomplish similar tasks. For example, we could have used Dataflow (based on Apache Beam) for our streaming solution. Or we could have gone with a completely different paradigm for how we store and query our data, such as storing our data as Parquet files in GCS and querying with Spark. I mention this here to make sure you understand that this book is not the complete guide to being a data engineer. Rather, it is an introduction to the types of problems a data engineer solves, and a sampling common tools in GCP used to solve those problems.

You can view the code we set up in this chapter in its final form in [Appendix A](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/appendix_a_example_code/README.md), which is a sample code repository.

This is going to be quite a long chapter, so here are the links for each section:
* Your Requirements
* Setting Up a New Project
* Mocking Hamsterwheel's Source Systems
* Instantiating Your Infrastructure with Terraform
* Writing Your Batch Pipeline with Composer/Airflow
* Writing Your Event-Driven Pipelines with Cloud Functions and Dataproc/Spark
* Writing your Streaming Pipeline with Pub/Sub, Cloud Functions, and GKE
* Deploying Your Code with Cloud Build
* Final Thoughts
* Cleaning Up

## Your Requirements

Welcome Hamsterwheel Batteries Inc., an up-and-coming battery retailer, where you are the newest (and only) data engineer for the company. Our analysts have decided that emailing Excel files to each other is no longer meeting their needs, so it's your job to set up a data warehouse and the various pipelines to feed that data. You'll need to bring in data from a variety of data sources, including:
* Web APIs
* CSV and JSON files periodically updated in GCS
* Batch data from the database supporting Hamsterwheel's website
* Streaming data from the database supporting Hamsterwheel's website

You'll need to store that data in the data lake and data warehouse you'll set up. You'll then need to transform that data into a format that is easy for our analysts to use. 

## Setting Up a New Project
So far in this book I've been working in `de-book-dev` project. In this chapter I'm going to build a production environment, so I'll make a new `de-book-prod` project. Keeping your environments in different projects is a good idea. For this chapter it's not strictly necessary, but it might be a good idea in case you have an lingering infrastructure on an existing project that may make it harder to track what you're building. Setting up a new project was discussed in Chapter 1.

We're also going to create a new GitHub repository for our code. I went over that in [Chapter 11: Deployment Pipelines with Cloud Build](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_11_deployment_pipelines.md). You can see mine here: https://github.com/Nunie123/hamsterwheel, though the same code is available in [Appendix A](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/appendix_a_example_code/README.md).

Now that we have a new project and a new repo, let's create a new service account and save our key in a top-level `keys/` folder in our repo. I discussed how to do this in Chapter 1. 

Because this code is going to be published to a remote repository we need to make sure our key file doesn't get made public. let's add a `.gitignore` file at the top level of our repo:
``` text
keys
__pycache__
.vscode
.DS_Store
```
The three entries below `keys` we aren't excluding because they are private, but rather because they contain cached data and settings that are not valuable to have in version control.

Finally, let's make sure we have the right account initialized with our new project set to default:
``` Bash
> gcloud init
```
If you don't have `gcloud` or the other GCP command line tools installed, check out Chapter 1.

## Mocking Hamsterwheel's Source Systems
Before we can dive into building our data pipelines we're going to need some source data to ingest. Because we don't have access to an actual company's data sources we are going to mock up some of our own. Yes, it does seem a bit silly to generate data in one place just so we can move it to another place. But by doing this we can actually see our infrastructure working, rather than building it and just taking my word that it would work.

In this section we will set up the systems and data we need to mock real data sources.

### Web API
For your Web API there's no fake data needed, we'll use ComEd's energy pricing API available here: https://hourlypricing.comed.com/api?type=5minutefeed

### CSV File
For our CSV files, let's suppose those are generated by the marketing department as potential leads for new customers. Hamsterwheel's marketing department puts a new file into a GCS bucket every morning. Let's start by making the CSV file using this Python script:
``` Python
# sale_leads.py
import csv
from faker import Faker

def generate_file() -> None:
    fake = Faker()
    with open('sale_leads.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['name', 'phone_number'])
        for _ in range(500):
            writer.writerow([fake.name(), fake.phone_number()])

if __name__ == '__main__':
    generate_file()
```
`faker` is a Python library for generating fake data. It is not part of the standard library, so you'll need to install it (`pip install Faker`). We're also going to need the Spanner Python library, so lets install that now as well. These should be installed inside a Python virtual environment, using the tool of your choice (e.g. [venv](https://docs.python.org/3/library/venv.html).
``` Bash
> pip install Faker
> pip install google-cloud-spanner
```

Now let's generate the CSV file by executing the script:
``` Bash
> python sale_leads.py
```
We can verify the output looks right:
``` Bash
> head -n 5 sale_leads.csv
name,phone_number
Brittany Walker,+1-152-651-8997x12442
Felicia Coleman,245.551.7982
Crystal Bell,803-316-1054x535
James Morris,354-636-3746
```

We need this file in a bucket (as if put there by Hamsterwheel's marketing department), so lets create a bucket and copy over the file:
``` Bash
> gsutil mb gs://de-book-source
> gsutil cp sale_leads.csv gs://de-book-source/marketing/
```
Note that the bucket I created I called `de-book-source`, but that name will likely not be available to you since all bucket names are unique on GCS. Choose whatever name you like.

### JSON File
For our JSON files, let's suppose Hamsterwheel runs a web scraper to get information on competitor products, and deposits a new file in a GCS bucket every evening after the script finishes running. We'll again use a python script to generate this file:
``` Python
# competitor_products.py
import json
import random
from faker import Faker

def generate_file() -> None:
    fake = Faker()
    data = []
    for _ in range(500):
        product = dict(
            company=fake.company(),
            product_name=' '.join(fake.words(2)),
            in_stock=fake.boolean(),
            sku=fake.ean(),
            price=random.randrange(500,20000)/100,
            product_groups=[[random.randrange(1,500), random.randrange(1,500)], [random.randrange(1,500), random.randrange(1,500)]]
        )
        data.append(product)
    with open('competitor_products.json', 'w') as f:
        json.dump(data, f)

if __name__ == '__main__':
    generate_file()
```
We can execute the script to generate the file, verify the contents, then copy the file to our GCS bucket:
``` Bash
> python competitor_products.py
> head -c 500 competitor_products.json
> gsutil cp competitor_products.json gs://de-book-source/scraper/
```

### Company Database

We have a Web API and a couple flat files, now for the hard part. We're going to set up a relational database to simulate Hamsterwheel's transactional database that tracks customer data and sales. We are going to use [Spanner](https://cloud.google.com/spanner), a fully managed relational database service on GCP to create a `customers` table and a `sales` table. We'll do a one-time load into our `customer` table, and set up a script to periodically add sales data to the `sales` table, which we'll use for our streaming pipeline.

We'll start by specifying our tables in [Data Definition Language (DDL)](https://cloud.google.com/spanner/docs/data-definition-language):
``` SQL
-- hamsterwheel_db.sql

CREATE TABLE customers (
    customer_id     STRING(36)      NOT NULL,
    customer_name   STRING(100)     NOT NULL,
    address         STRING(1000)    NULL,
    phone_number    STRING(20)      NULL,
    is_active       BOOL            NOT NULL    AS  TRUE
) PRIMARY KEY (customer_id)
;

CREATE TABLE sales(
    sale_id         STRING(36)      NOT NULL,
    sale_price      FLOAT64         NOT NULL,
    sale_timestamp  TIMESTAMP       NOT NULL,
    customer_id     STRING(36)      NOT NULL
    ) PRIMARY KEY (sale_id)
    , FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
;
```
Now we can instantiate the database:
``` Bash
> gcloud spanner databases create hamsterwheel-db \
    --instance=hamsterwheel-instance \
    --ddl-file=hamsterwheel_db.sql
```

We have our tables set up, so now we can insert some data. We're going to use some Python scripts to generate and insert our data.
``` Python
# customer_data.py
from faker import Faker
from google.cloud import spanner

fake = Faker()

def generate_insert_statement() -> str:
    sql_list = ['insert into customers(customer_id, customer_name, address, phone_number, is_active)']
    sql_list.append('values')
    for i in range(1, 501):
        row = f'({i}, {fake.name()}, {fake.address()}, {fake.phone_number()}, {fake.boolean})'
        sql_list.append(row)
    sql_statement = '\n'.join(sql_list)
    return sql_statement

def insert_customer_records_callback(transaction) -> None:
    sql_statement = generate_insert_statement()
    row_ct = transaction.execute_update(sql_statement)
    print(f"{row_ct} records inserted.".)

def insert_custom_records():
    spanner_client = spanner.Client()
    instance = spanner_client.instance('hamsterwheel-instance')
    database = instance.database('hamsterwheel-db')
    database.run_in_transaction(insert_customer_records_callback)

if __name__ == '__main__':
    insert_customer_records()
```
Let's execute it:
``` Bash
> python customer_data.py
```

For sales data, we want to set up a long-running Python script that will periodically be adding new records:
``` Python
# sales_data.py
import uuid
import random
import datetime
import time
from google.cloud import spanner

def generate_insert_statement() -> str:
    sql_list = ['insert into sales(sale_id, sale_price, sale_timestamp, customer_id)']
    sql_list.append('values')
    row = f'({uuid.uuid4()}, {random.randrange(500,20000)/100}, {datetime.datetime.now()}, {random.randint(1,500)})'
    sql_list.append(row)
    sql_statement = '\n'.join(sql_list)
    return sql_statement

def insert_product_record_callback(transaction) -> None:
    sql_statement = generate_insert_statement()
    row_ct = transaction.execute_update(sql_statement)
    print(f"{row_ct} record inserted.".)

def insert_product_records_over_time():
    spanner_client = spanner.Client()
    instance = spanner_client.instance('hamsterwheel-instance')
    database = instance.database('hamsterwheel-db')
    while i < 240:
        database.run_in_transaction(insert_product_record_callback)
        i = i + 1
        sleep_time = random.randint(20,40)
        time.sleep(sleep_time)

if __name__ == '__main__':
    insert_product_records_over_time()
```
``` Bash
> python sales_data.py
```
This script is set up to run for about two hours. We can just leave it alone and open a new terminal session to use for the rest of the chapter.

Finally, let's make sure our data is in our tables:
``` Bash
> gcloud spanner databases execute-sql hamsterwheel-db \
    --instance=hamsterwheel-instance \
    --sql="select * from customers limit 20"
> gcloud spanner databases execute-sql hamsterwheel-db \
    --instance=hamsterwheel-instance \
    --sql="select * from sales limit 20"
```

We now have all the data sources we need to set up realistic data engineering infrastructure. So let's get started.

## Instantiating Your Infrastructure with Terraform
As the sole data engineer at Hamsterwheel Batteries Inc. it's your job to maintain all of the data engineering infrastructure. That includes debugging infrastructure failures and recovering from disasters (e.g. a backhoe severs the internet connection of the GCP location where your infrastructure is running). Both of those tasks (debugging, disaster recovery) get a lot easier if you know exactly what infrastructure you are running at any given time. As we talked about in [Chapter 10: Infrastructure as Code with Terraform](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_10_infrastructure_as_code.md), we can solve those problems by adopting Infrastructure as Code with Terraform.

We installed Terraform in [Chapter 10: Infrastructure as Code with Terraform](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_10_infrastructure_as_code.md). If you don't have it installed refer to [Chapter 10: Infrastructure as Code with Terraform](https://github.com/Nunie123/data_engineering_on_gcp_book/blob/master/ch_10_infrastructure_as_code.md) or follow the [online instructions](https://learn.hashicorp.com/tutorials/terraform/install-cli).

Now we need to plan out what infrastructure we are going to need:
* You are presenting your data to users in a data warehouse, so you'll need BigQuery.
* You need a data lake to store your raw data, so you'll need Google Cloud Storage (GCS).
* You need to ingest data files once they are loaded into a GCS bucket, so you'll need Cloud Functions.
* You need to transform your data before loading it into BigQuery, so you'll need Dataproc/Spark.
* You need to ingest data at regular intervals from a web API and a database, so you'll need Composer/Airflow.
* You need to stream data from a database into BigQuery, so you'll need a GKE cluster for querying the database, Pub/Sub for queueing the data, and a Cloud Function to insert it into BigQuery.
* You need a deployment pipeline to get your code into GCP, so you'll need Cloud Build.
* You need to make sure the services you are using have permission to do the things they need and no more, so you'll need to set up service accounts.

This is a good list to get you started. It's easy enough to change your infrastructure as you go (another benefit of Terraform).

Now let's get down to writing our Terraform file. You'll start by creating a `terraform` folder at the top level of your repo, and while you're at ti you can create your main.tf file.
``` Bash
> mkdir terraform
> touch terraform/main.tf
```
And because you don't want any unhelpful terraform cache files cluttering up your repo, you'll update your `.gitignore` file:
``` Text
keys
__pycache__
.vscode
.DS_Store
.tfstate
.terraform
.tfplan
```
Now we are ready to fill in main.tf:
``` JS


















```
That was a lot. As your infrastructure grows you can split this Terraform file into multiple files (e.g. a single file for BigQuery). You can also take advantage of Terraform's [modules](https://www.terraform.io/docs/language/modules/develop/index.html) to reduce boilerplate code. Another optimization is to take advantage of Terraform [variables](https://www.terraform.io/docs/language/values/index.html) to have the same Terraform code instantiate multiple environments (e.g. Dev and Prod).

Now that you have your infrastructure defined, you need to deploy it. Deploying with Terraform should be done manually on the command line, so that you can review any changes before applying your configuration. In the same directory as your `main.tf` file execute:
``` Bash
> terraform init
> terraform apply
```
After reviewing your plan, enter "yes" when prompted.

## Writing Your Batch Pipelines with Composer/Airflow
Your Composer Environment is running now, but it's not going to do anything until you write some DAGs. A good way to organize your DAGs is to make one DAG per pipeline.

You have four data sources that you are going to batch load, and you need a separate pipeline for each:
1. Energy pricing data from ComEd's web API
2. Customer data from Hamsterwheel's Spanner DB
3. Marketing leads data from GCS
4. Competitor products data from GCS

At the top level of your repo create your `dags` folder and `helpers` subfolder:
``` Bash
> mkdir dags
> mkdir dags/helpers
```

In your text editor of choice create a `settings.py` file in the `helpers` folder:
``` Python
# settings.py

# These are the default settings to be used for every DAG. They can be overwritten in the DAG file if needed.
DEFAULT_DAG_ARGS = {
    'owner': 'DE Book',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=30),
    'start_date': datetime.datetime(2020, 10, 17),
}

# Composer maps this file path to a GCS bucket so it can be treated like local storage.
LOCAL_STORAGE = '/home/airflow/gcs/data/'
DAGS_FOLDER = '/home/airflow/gcs/dags/'
```

Now add these four files in your `dags` folder.

`comed.py`:
``` Python
#! /usr/bin/env python3
""" 
comed.py
This DAG pulls energy pricing data from https://hourlypricing.comed.com/api?type=5minutefeed. That data will be saved
to GCS, then uploaded to BQ.
"""
import datetime
import json
import os
import textwrap

from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator

from helpers import settings

dag = DAG(
    'comed',
    schedule_interval="0 0 * * *",   # run every day at midnight UTC
    max_active_runs=1,
    catchup=False,
    default_args=settings.DEFAULT_DAG_ARGS
)

LOCAL_FILE = os.path.join(settings.LOCAL_STORAGE, 'energy_prices.json')
COMED_URL = 'https://hourlypricing.comed.com/api?type=5minutefeed'
TODAY_NODASHES = datetime.date.today().strftime('%Y%m%d')
GCS_LOCATION = os.path.join('gs://de-book-prod/comed', TODAY_NODASHES, 'energy_prices.json')
SCHEMA_FILE = os.path.join(settings.DAGS_FOLDER, 'schemas', 'comed_pricing_lnd.json')


def download_data_to_local(url: str, destination: str) -> None:
    """
    This function sends an HTTP GET request to the provided URL, and saves the returned data to the provided destination as a newline delimited JSON file.
    ## PARAMETERS ##
    url: String. The URL to which and HTTP GET request will be sent. e.g. "http://www.example.com/api/"
    destination: String. The file location where the data will be saved. e.g. "/path/to/local/file.txt"
    ##
    returns: None
    """
    import requests
    import pandas as pd
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    df.to_json(destination, orient='records', lines=True)


t_download_data_to_local = PythonVirtualenvOperator(
    task_id="download_data_to_local",
    python_version="3",
    python_callable=download_data_to_local,
    requirements=["requests==2.7.0", "pandas==1.1.3"],
    op_kwargs={
        url=COMED_URL,
        destination=LOCAL_FILE
    }
    dag=dag
)

t_upload_data_to_gcs = BashOperator(
    task_id="upload_data_to_gcs",
    bash_command=f"gsutil cp {LOCAL_FILE} {GCS_LOCATION}",
    dag=dag
)
t_upload_data_to_bq.set_upstream(t_download_data_to_local)

t_upload_data_to_bq = BashOperator(
    task_id="upload_data_to_bq",
    bash_command=textwrap.dedent(f"""\
        bq load \\
        --source_format=NEWLINE_DELIMITED_JSON \\
        --time_partitioning_type DAY \\
        --replace \\
        'competitors.comed_pricing_lnd${TODAY_NODASHES}' \\
        '{GCS_LOCATION}' \\
        '{SCHEMA_FILE}'
    """),
    dag=dag
)
t_upload_data_to_bq.set_upstream(t_upload_data_to_gcs)
```


`hamsterwheel_db.py`:
``` Python
#! /usr/bin/env python3
""" 
hamsterwheel_db.py
This DAG pulls customer data from a replica of Hamsterwheel's application DB. That data will be saved
to GCS, then uploaded to BQ.
"""
import datetime
import json
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

from helpers import settings

dag = DAG(
    'comed',
    schedule_interval="0 * * * *",   # run every hour, on the hour
    max_active_runs=1,
    catchup=False,
    default_args=settings.DEFAULT_DAG_ARGS
)

LOCAL_FILE = os.path.join(settings.LOCAL_STORAGE, 'customers.json')
CUSTOMERS_SQL = "SELECT customer_id, customer_name, address, phone_number, is_active FROM customers"
TODAY_NODASHES = datetime.date.today().strftime('%Y%m%d')
GCS_LOCATION = os.path.join('gs://de-book-prod/comed', f'{TODAY_NODASHES}/')
SCHEMA_FILE = os.path.join(settings.DAGS_FOLDER, 'schemas', 'customer_lnd.json')

def save_data_to_local(sql: str, destination: str) -> None:
    """
    This function takes the provided SQL, executes it against the Hamsterwheel Spanner DB, then saves
    the results at the provided location as newline-delimited JSON.
    ## PARAMETERS ##
    sql: String. The SQL query to be executed. e.g. "select * from my_table"
    destination: String. The file location where the data will be saved. e.g. "/path/to/local/file.txt"
    ##
    returns: None
    """
    from google.cloud import spanner
    import pandas as pd
    spanner_client = spanner.Client()
    instance = spanner_client.instance('hamsterwheel-instance')
    database = instance.database('hamsterwheel-db')
    customers = []
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(sql)
        for row in results:
            customer = dict(
                customer_id = row[0]
                customer_name = row[1]
                address = row[2]
                phone_number = row[3]
                is_active = row[4]
            )
            customers.append(customer)
    df = pd.DataFrame(customers)
    df.to_json(destination, orient='records', lines=True)

t_save_data_to_local = PythonVirtualenvOperator(
    task_id="save_data_to_local",
    python_version="3",
    python_callable=save_data_to_local,
    requirements=["google-cloud-spanner==3.0.0"],
    op_kwargs={
        sql=CUSTOMERS_SQL,
        destination=LOCAL_FILE
    }
    dag=dag
)

t_copy_data_to_lake = BashOperator(
    task_id="copy_data_to_lake",
    bash_command=f"gsutil cp {LOCAL_FILE} gs://de-book-prod/comed/{TODAY_NODASHES}/",
    dag=dag
)
t_copy_data_to_lake.set_upstream(t_save_data_to_local)

t_upload_data_to_bq = BashOperator(
    task_id="upload_data_to_bq",
    bash_command=textwrap.dedent(f"""\
        bq load \\
        --source_format=NEWLINE_DELIMITED_JSON \\
        --time_partitioning_type DAY \\
        --replace \\
        'competitors.comed_pricing_lnd${TODAY_NODASHES}' \\
        '{GCS_LOCATION}' \\
        '{SCHEMA_FILE}'
    """),
    dag=dag
)
t_upload_data_to_bq.set_upstream(t_copy_data_to_lake)
```

`competitor_products.py`:
``` Python
#! /usr/bin/env python3
""" 
competitor_products.py
This DAG pulls competitor product data from a source GCS bucket, moves it to a GCS bucket in our data lake,
process the file in Dataproc to remove the array of arrays that is incompatible with BigQuery, save the 
cleaned file to GCS, then load to BigQuery.
"""
import datetime
import json
import os
import textwrap

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from helpers import settings

dag = DAG(
    'competitor_products',
    schedule_interval=None,   # Not scheduled. Will be triggered by Cloud Function
    max_active_runs=1,
    catchup=False,
    default_args=settings.DEFAULT_DAG_ARGS
)

SOURCE_GCS_DESTINATION = 'gs://de-book-source/scraper/competitor_products.json'
TODAY_NODASHES = datetime.date.today().strftime('%Y%m%d')
DESTINATION_GCS_LOCATION = os.path.join('gs://de-book-prod/competitors', TODAY_NODASHES, 'raw', 'competitor_products.json')
CLEAN_GCS_LOCATION = os.path.join('gs://de-book-prod/competitors', TODAY_NODASHES, 'clean', 'competitor_products.json')
SCHEMA_FILE = os.path.join(settings.DAGS_FOLDER, 'schemas', 'competitor_products_lnd.json')


t_copy_raw_data_to_gcs = BashOperator(
    task_id="copy_raw_data_to_gcs",
    bash_command=f"gsutil cp {SOURCE_GCS_DESTINATION} {DESTINATION_GCS_LOCATION}",
    dag=dag
)

# using Spark because we expect these files to be quite large
t_start_dataproc_cluster = BashOperator(
    task_id="start_dataproc_cluster",
    bash_command=textwrap.dedent("""
        gcloud dataproc clusters create competitor-products-cluster \\
        --region=us-central1 \\
        --num-workers=2 \\
        --worker-machine-type=n2-standard-2 \\
        --image-version=1.5-debian10
    """),
    dag=dag
)

t_submit_pyspark_job = BashOperator(
    task_id="submit_pyspark_job",
    bash_command=textwrap.dedent("""
        gcloud dataproc jobs submit pyspark \\
        gs://de-book-prod/pyspark_jobs/competitor_products.py \\
        --cluster=my-cluster \\
        --region=us-central1
    """),
    dag=dag
)
t_submit_pyspark_job.set_upstream(t_start_dataproc_cluster)
t_submit_pyspark_job.set_upstream(t_copy_raw_data_to_gcs)

t_delete_dataproc_cluster = BashOperator(
    task_id="submit_pyspark_job",
    bash_command="gcloud dataproc clusters delete competitor-products-cluster --region=us-central1",
    dag=dag
)
t_delete_dataproc_cluster.set_upstream(t_submit_pyspark_job)

t_upload_data_to_bq = BashOperator(
    task_id="upload_data_to_bq",
    bash_command=textwrap.dedent(f"""\
        bq load \\
        --source_format=CSV \\
        --time_partitioning_type DAY \\
        --replace \\
        --skip_leading_rows 1 \\
        'marketing.competitor_products_lnd${TODAY_NODASHES}' \\
        '{CLEAN_GCS_LOCATION}' \\
        '{SCHEMA_FILE}'
    """),
    dag=dag
)
t_upload_data_to_bq.set_upstream(t_submit_pyspark_job)
```

`sales_leads.py`:
``` Python
#! /usr/bin/env python3
""" 
sales_leads.py
This DAG pulls sales lead data from the marketing GCS bucket, brings it into the data lake bucket, then moves it to BigQuery.
"""
import datetime
import json
import os
import textwrap

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from helpers import settings

dag = DAG(
    'sales_leads',
    schedule_interval=None,   # Not scheduled. Will be triggered by Cloud Function
    max_active_runs=1,
    catchup=False,
    default_args=settings.DEFAULT_DAG_ARGS
)

SOURCE_GCS_DESTINATION = 'gs://de-book-source/marketing/sales_leads.csv'
TODAY_NODASHES = datetime.date.today().strftime('%Y%m%d')
DESTINATION_GCS_LOCATION = os.path.join('gs://de-book-prod/marketing', TODAY_NODASHES, 'sales_leads.csv')
SCHEMA_FILE = os.path.join(settings.DAGS_FOLDER, 'schemas', 'sales_leads_lnd.json')


t_copy_data_to_gcs = BashOperator(
    task_id="copy_data_to_gcs",
    bash_command=f"gsutil cp {SOURCE_GCS_DESTINATION} {DESTINATION_GCS_LOCATION}",
    dag=dag
)

t_upload_data_to_bq = BashOperator(
    task_id="upload_data_to_bq",
    bash_command=textwrap.dedent(f"""\
        bq load \\
        --source_format=CSV \\
        --time_partitioning_type DAY \\
        --replace \\
        --skip_leading_rows 1 \\
        'marketing.sales_leads_lnd${TODAY_NODASHES}' \\
        '{DESTINATION_GCS_LOCATION}' \\
        '{SCHEMA_FILE}'
    """),
    dag=dag
)
t_upload_data_to_bq.set_upstream(t_upload_data_to_gcs)
```

Those are the DAGs for your four batch pipelines. There are a couple a python functions inside those DAGs, so you should create some unit tests. Start by adding a `tests` folder inside your `dags` folder, then create a `dag_tests.py` file:
``` Python
# dag_tests.py
# run from top level of repo
import unittest
from unittest import mock
import json

from ..comed import download_data_to_local
from ..hamsterwheel_db import save_data_to_local

class TestComedDag(unittest.TestCase):

    @mock.patch('requests.get')
    def download_data_to_local(self, mock_request):
        mock_data = [
            {'millisUTC': '1612738500000', 'price': '3.1'},
            {'millisUTC': '1612738600000', 'price': '3.2'},
            {'millisUTC': '1612738700000', 'price': '3.3'}
        ]
        mock_request.return_value.json.return_value = mock_data
        mock_request.return_value.ok = True

        local_file = 'test_output.json'
        url = 'http://www.example.com/api/'
        download_data_to_local(url, local_file)
        data_list = []
        with open(local_file, r) as f:
            for line in f:
                data_dict = json.load(line)
                data_list.append(data_dict)
        
        self.assertEqual(len(data_list), 3)
        self.assertEqual(data_list[0]["price"], '3.1')
        self.assertEqual(data_list[0]["millisUTC"], '1612738500000')
        
    def tearDown(self):
        os.remove('test_output.json')


class TestHamsterwheelDbDag(unittest.TestCase):

    @mock.patch('snapshot.execute_sql')
    def save_data_to_local(self, mock_query):
        mock_data = [
            [1, 'Sam', '1600 Pennsylvania Ave.', '555-555-1111', False],
            [2, 'Josh', '5 Sesame St.', '555-555-2222', True],
            [3, 'Toby', '1 Main St.', '555-555-3333', True]
        ]
        mock_query.return_value = mock_data

        sql = 'select foo from bar'
        local_file = 'test_output.json'
        save_data_to_local(sql, local_file)
        data_list = []
        with open(local_file, r) as f:
            for line in f:
                data_dict = json.load(line)
                data_list.append(data_dict)
        
        self.assertEqual(len(data_list), 3)
        self.assertEqual(len(data_list[0]), 5)
        self.assertEqual(data_list[1]["customer_name"], 'Josh')
        self.assertTrue(data_list[2]["is_acrtive"])

    def tearDown(self):
        os.remove('test_output.json')


if __name__ == '__main__':
    unittest.main()
```

Those DAGs will control your batch pipelines. But two of those DAGs rely on Cloud Functions to trigger, and one of those also needs to run a Spark job. You'll need to write these before all your batch pipelines will work.
## Writing Your Event-Driven Pipelines with Cloud Functions and Dataproc/Spark
To finish your batch pipelines you'll need the following:
1. A Cloud Function to trigger the Sales Leads DAG.
2. A Cloud Function to trigger the Competitor Products DAG.
3. A Spark job to process the competitor products data.

At the top level of your repo create your file structure by executing the following:
``` Bash
> mkdir functions
> mkdir functions/sales_leads
> touch functions/sales_leads/main.py
> touch functions/sales_leads/requirements.txt
> mkdir functions/competitor_products
> touch functions/competitor_products/main.py
> touch functions/competitor_products/requirements.txt
```
To trigger a DAG from your Cloud Function, you need to get your Airflow Client ID. To do that, create and execute this Python script:
``` Python
# client_id.py
import google.auth
import google.auth.transport.requests
import requests
import six.moves.urllib.parse

# Authenticate with Google Cloud.
# See: https://cloud.google.com/docs/authentication/getting-started
credentials, _ = google.auth.default(
    scopes=['https://www.googleapis.com/auth/cloud-platform'])
authed_session = google.auth.transport.requests.AuthorizedSession(credentials)

project_id = 'de-book-prod'
location = 'us-central1'
composer_environment = 'batch_jobs'

environment_url = (
    'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
    '/environments/{}').format(project_id, location, composer_environment)
composer_response = authed_session.request('GET', environment_url)
environment_data = composer_response.json()
airflow_uri = environment_data['config']['airflowUri']

# The Composer environment response does not include the IAP client ID.
# Make a second, unauthenticated HTTP request to the web server to get the
# redirect URI.
redirect_response = requests.get(airflow_uri, allow_redirects=False)
redirect_location = redirect_response.headers['location']

# Extract the client_id query parameter from the redirect.
parsed = six.moves.urllib.parse.urlparse(redirect_location)
query_string = six.moves.urllib.parse.parse_qs(parsed.query)
print(query_string['client_id'][0])
```
``` Bash
> python client_id.py
abc123
```

In addition to your Client ID, you'll need your Webserver ID:
``` Bash
> gcloud composer environments describe batch_jobs \
    --location us-central1 \
    --format="get(config.airflowUri)"
```
Your Webserver ID is the first part of your webserver URL. e.g. `<Webserver ID>.appspot.com`

In the `main.py` file for Sales Leads you'll need:
``` Python
# This code is lifted from: https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf

from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests


IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'


def trigger_dag(data, context=None):
    """Makes a POST request to the Composer DAG Trigger API

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.

    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python

    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.
    """

    # Fill in with your Composer info here
    # Navigate to your webserver's login page and get this from the URL
    # Or use the script found at
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/composer/rest/get_client_id.py
    client_id = 'YOUR-CLIENT-ID'
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = 'YOUR-TENANT-PROJECT'
    # The name of the DAG you wish to trigger
    dag_name = 'sales_leads'
    webserver_url = (
        'https://'
        + webserver_id
        + '.appspot.com/api/experimental/dags/'
        + dag_name
        + '/dag_runs'
    )
    # Make a POST request to IAP which then Triggers the DAG
    make_iap_request(
        webserver_url, client_id, method='POST', json={"conf": data, "replace_microseconds": 'false'})


# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE
def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text
# END COPIED IAP CODE
```
Be sure to fill in your Client ID and Webserver ID in the code above.

In the same folder, create `requirements.txt`:
``` Text
requests_toolbelt==0.9.1
google-auth==1.24.0
```

In the `main.py` file for Competitor Products you'll need:
``` Python
# This code is lifted from: https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf

from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests


IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'


def trigger_dag(data, context=None):
    """Makes a POST request to the Composer DAG Trigger API

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.

    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python

    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.
    """

    # Fill in with your Composer info here
    # Navigate to your webserver's login page and get this from the URL
    # Or use the script found at
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/composer/rest/get_client_id.py
    client_id = 'YOUR-CLIENT-ID'
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = 'YOUR-TENANT-PROJECT'
    # The name of the DAG you wish to trigger
    dag_name = 'competitor_products'
    webserver_url = (
        'https://'
        + webserver_id
        + '.appspot.com/api/experimental/dags/'
        + dag_name
        + '/dag_runs'
    )
    # Make a POST request to IAP which then Triggers the DAG
    make_iap_request(
        webserver_url, client_id, method='POST', json={"conf": data, "replace_microseconds": 'false'})


# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE
def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text
# END COPIED IAP CODE
```
You can use the same Client ID and Webserver ID you used above.

Again, you'll need to create `requirements.txt` in the same folder:
``` Text
requests_toolbelt==0.9.1
google-auth==1.24.0
```
These two cloud functions are largely identical. However, because these functions are being deployed independently you cannot DRY out this code by abstracting the common code into a shared module. A potential enhancement is to create a single Cloud Function to trigger DAGs, and infer the DAG name from the event data, such as the name of the bucket a file was loaded into.

The last piece of your batch pipelines is your Spark job for the competitor products data. This is necessary because the JSON schema contains an array-of-arrays, which is a structure that cannot be loaded into BigQuery. You are using Spark for this (as opposed to e.g. Pandas) because you suspect that this file will periodically be too large to fit into memory.

Start by adding the `pyspark_jobs` folder to the top level of your repo. Inside `pyspark_jobs` create `competitor_products.py`:
``` Python
#!/usr/bin/env python
"""
This pyspark job takes a JSON file of competitor products and transforms it. Each record in that file contains the "product_groups"
field, which is an array-of-arrays. This script will change that field to an array-of-strings by concatenating the inner array.
The other fields will be unchanged.
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws


def generate_clean_blob_path(bucket_name: str, blob_prefix: str) -> str:
    """
    This function is used to generate the path that the clean file will be saved to. For example, the raw file may be
    read from "gs://my-bucket/competitor_products/20211225/raw/competitor_products.json", and the transformed file will be
    saved to "gs://my-bucket/competitor_products/20211225/clean/".
    ## PARAMETERS ##
    bucket_name: String. Name of GCS bucket where the data exists.
    blob_prefix: String. Name of the blob, including folders within the bucket, if any.
    ##
    returns: String. The GCS path to save the transformed data.
    """
    new_blob_prefix = os.path.dirname(os.path.dirname(blob_prefix))
    destination = os.path.join('gs://', new_blob_prefix, "clean")
    return destination


def main(bucket_name: str, blob_prefix: str) -> str:
    """
    This function reads a competitor_products.json file into a dataframe, transform the "product_groups"
    field, then saves the data back to GCS.
    ## PARAMETERS ##
    bucket_name: String. Name of GCS bucket where the data exists.
    blob_prefix: String. Name of the blob, including folders within the bucket, if any.
    ##
    returns: None
    """
    spark = SparkSession.builder.appName("competitor_products").getOrCreate()
    df = spark.read.json(f"gs://{bucket_name}/{blob_prefix}")
    df2 = df.withColumn("product_groups", concat_ws("; ", col("product_groups")))
    destination = generate_clean_blob_path(bucket_name, blob_prefix)
    df2.write.json(destination, mode="overwrite")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket_name", dest="bucket_name", required=True,
        help="Name of GCS bucket where the data exists."
        )
    parser.add_argument(
        "--blob_prefix", dest="blob_prefix", required=True,
        help="Name of the blob, including folders within the bucket, if any."
        )
    args = parser.parse_args()

    main(bucket_name=args.bucket_name, blob_prefix=args.blob_prefix)

```

## Writing your Streaming Pipeline with Pub/Sub, Cloud Functions, and GKE

## Deploying Your Code with Cloud Build
Now you need to get your code onto GCP so your data will start flowing. You need to give it code to execute so it can start moving and storing data. In particular you need your deployment process to do the following:
1. Execute your automated tests
2. Deploy your Cloud Functions
3. Deploy your Composer DAGs
4. Deploy your Dataproc job
5. Deploy your GKE image

**A note about GKE:** You may have noticed that I did not cover Google Kubernetes Engine (GKE) previously in this book. GKE is a powerful tool, but it's also a big topic to cover, so I left it out for the sake of brevity. We'll be using it here, and this code should be a good starting point if you want to learn more. I mention this only so you don't waste your time looking for where I covered GKE in more detail in previous chapters, because I didn't.

Before you can complete your Cloud Build file you'll need a few things. First, you'll need the bucket name where Composer will look for new DAGs:
``` Bash
> gcloud composer environments describe prod_environment \
    --location us-central1 \
    --format="get(config.dagGcsPrefix)"
gs://us-central1-prod-environment-abc123456-bucket/dags
```

In the top level of your repo create your `cloudbuild.yaml` file:
``` YAML
# cloudbuild.yaml
steps:

# run automated tests
- name: 'docker.io/library/python:3.7'
  id: Test
  entrypoint: /bin/sh
  args: [-c, 'python -m unittest dags/tests/']

# deploy DAGs
- name: gcr.io/cloud-builders/gcloud
  id: Deploy
  entrypoint: bash
  args: [ '-c', 'gsutil -m rsync -d -r ./dags gs://${_COMPOSER_BUCKET}/dags']
substitutions:
    _COMPOSER_BUCKET: us-central1-prod-environment-abc123456-bucket  # You retrieved this value above

# build the GKE container image
- name: "gcr.io/cloud-builders/docker"
  args: ["build", "-t", "gcr.io/project-id/image:tag", "."]

# push the GKE container image
- name: "gcr.io/cloud-builders/docker"
  args: ["push", "gcr.io/project-id/image:tag"]

# deploy container image to GKE
- name: "gcr.io/cloud-builders/gke-deploy"
  args:
  - run
  - --filename=kubernetes-resource-file
  - --image=gcr.io/project-id/image:tag
  - --location=location
  - --cluster=cluster

```


## Final Thoughts

## Cleaning Up