# airflow_etl

#Install dependent libraries:

pip install apache-airflow
pip install email_validator

#Clone the repository: 
git clone https://github.com/kademarsharath/airflow_home.git

#Export the home path: 
export AIRFLOW_HOME=\`pwd\`/airflow_home

#Navigate to homepath:

cd airflow_home;


#Execute the below command to see the pipeline on webserver (You can add "- D" to run in background:

airflow webserver 

#Execute the below to test indiavidual dag:

airflow test  test_dag ETL 2020-04-27
