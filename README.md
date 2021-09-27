# End-to-End-ML-pipeline-using-Amazon-Sagemaker-and-Glue
End to end ML pipleline using amazon Sagemaker and Glue

This tutorial will show you the step by step guide on how to bringing your Pyspark processing jobs and pre-trained model and deploy and host your model, in an automated pipeline using Glue workflow. In particular, when you have already created spark processing job and a pre-trained model that you would like to bring your scrip and host on Sagemaker.

We will build an end-to-end pipeline to predict the type of Iris using the famous iris data. Our pipeline will combine several different AWS services, use AWS Glue for serverless extract-transform-load (ETL) jobs, Amazon SageMaker for Machine learning and Amazon Simple Storage Service (S3) for storage and staging the datasetsto make inferences.

In addition to training ML models using existing SageMaker functionality, you will learn how to use SageMaker APIs to launch an AutoPilot job from a Jupyter environment, which allows you to automatically build, train and tune ML models.

By the end of this lab, your data scientists and analysts will learn how to directly obtain predictions from ML models using just SQL queries. The inferences can directly then be ingested into a database for OLAP or business intelligence.

![image](https://user-images.githubusercontent.com/9032900/109739694-1bab3400-7c1e-11eb-96bf-160228bf505f.png)

Table of content
- Create S3 buckets

- Create Glue job for extracting and storing raw data

- Create Glue Job to transform the data

- Bring your own model script (SKlearn model) and train and deploy on Sagemaker
