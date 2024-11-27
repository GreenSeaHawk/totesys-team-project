[![Contributors](https://img.shields.io/github/contributors/GreenSeaHawk/totesys-team-project?style=for-the-badge)](https://github.com/GreenSeaHawk/totesys-team-project/graphs/contributors)
[![Forks](https://img.shields.io/github/forks/GreenSeaHawk/totesys-team-project?style=for-the-badge)](https://github.com/GreenSeaHawk/totesys-team-project/network/members)
[![Issues](https://img.shields.io/github/issues/GreenSeaHawk/totesys-team-project?style=for-the-badge)](https://github.com/GreenSeaHawk/totesys-team-project/issues)


# Totesys ETL Pipeline

## Introduction
This project is the product of the Northcoders group project. The task was to create a data platform for Terrific Totes to improve querying and business intelligence operations. This data platform is in the form of a ETL (extract, transform, load) pipeline, this pipeline extracts data from [totesys database](https://dbdiagram.io/d/SampleDB-6332fecf7b3d2034ffcaaa92), transforms the data and loads it into a [data warehouse](https://dbdiagram.io/d/RevisedDW-63a19c5399cb1f3b55a27eca) for further analyis.

## **Tech Stack**

- **Programming Language**: Python
- **Visualisation**: Matplotlib, Jupyter Notebook
- **Containerisation**: Docker
- **AWS Cloud Services**: CloudWatch, ECR, Event Bridge, Lambda, Secrets Manager, SNS, Step Function, S3
- **CI/CD**: GitHub Actions
- **Infrastructure as Code**: Terraform
- **Libraries**: Pandas, NumPy, PG8000, Boto3, Pytest, PyArrow, SQLAlchemy

## Features
- Secrets management using **AWS Secrets Manager** for secure data handling
- Automated ETL pipeline for transforming raw data into structured datasets
- Cloud infrastructure created using **Terraform** for automated deployment
- Containerised deployment of application code and dependencies using **Docker**
- Executed tasks using serverless **AWS lambda** for scalability and lower costs
- Centralised logging and error monitoring using **AWS CloudWatch** and **AWS SNS**
- Stores raw and transformed data in **S3 buckets** to allow historical data tracking
- **GitHub actions** to automate testing and deployment of pipeline for seamless updates
- Data visualisation using **Matplotlib**

## Deployment
To enable the CI/CD pipeline, you need to set up **GitHub Secrets** in your repository:

### **Setting Up GitHub Secrets**
1. Go to your repository on GitHub.
2. Navigate to **Settings > Secrets and variables > Actions**.
3. Click on **New repository secret** for each variable and add the following:
   - **Name**: Enter the exact names (`AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ACCOUNT_ID`).
   - **Value**: Enter the corresponding values: 
      1. **`AWS_ACCESS_KEY`**:  AWS access key for programmatic access to AWS services.
      2. **`AWS_SECRET_ACCESS_KEY`**:  The secret access key corresponding to the AWS access key.
      3. **`AWS_REGION`**:  The AWS region where your infrastructure will be deployed (e.g., `eu-west-2`).
      4. **`AWS_ACCOUNT_ID`**:  Your AWS account ID, required for pushing Docker images to Amazon ECR.

### **Creating secrets in AWS Secrets Manager**
As part of deployment the pipeline requires secrets for database connections in AWS secrets manager. Create the following secrets:
1. **`my-database-connection`**:
   - Stores the connection details for the source OLTP database.
   - Example:
     ```json
     {
       "host": "my-database.amazonaws.com",
       "port": 5432,
       "username": "db_user",
       "password": "db_password",
       "database": "my_database"
     }
     ```

2. **`my-datawarehouse-connection`**:
   - Stores the connection details for the target data warehouse.
   - Example:
     ```json
     {
       "host": "my-datawarehouse.amazonaws.com",
       "port": 5432,
       "username": "warehouse_user",
       "password": "warehouse_password",
       "database": "my_datawarehouse"
     }
     ```
### Trigger deployment
1. Push changes to main branch
2. Monitor deployment progress in GitHub actions