import boto3
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
import pandas as pd
#import s3fs

#!/usr/bin/env python
# coding: utf-8

# ## Set up


import boto3
import sys
from datetime import datetime


# ### Configure


region = 'ap-southeast-2'

data_bucket_name = "34343-processed"
code_bucket_name = "my-byomodel"
model_bucket_name= "my-byomodel"

training_job_prefix = "demo-sklearn-boto3"

entry_point = 'aws_sklearn_main.py'


# In[35]:

s3_src_tar_uri = "s3://{}/source/sourcedir.tar.gz".format(code_bucket_name)
s3_train_data_uri = "s3://{}/train/train.csv".format(data_bucket_name)
s3_output_model_uri = "s3://{}/{}/models".format(model_bucket_name,training_job_prefix)


print(s3_train_data_uri)
print(s3_src_tar_uri)


# ## Train

# In[36]:


account_id =  boto3.client('sts').get_caller_identity().get('Account')
training_job_name='{}-{}'.format(training_job_prefix, datetime.now().strftime("%Y%m%d%H%M%S"))
role_arn= 'arn:aws:iam::571660658801:role/service-role/AmazonSageMaker-ExecutionRole-20210122T122859'


# In[37]:


aws_sagemaker_region_table_map ={
"us-west-1":746614075791,
"us-west-2":246618743249,
"us-east-1":683313688378,
"us-east-2":257758044811,
"ap-northeast-1":354813040037,
"ap-northeast-2":366743142698,
"ap-southeast-1":121021644041,
"ap-southeast-2":783357654285,
"ap-south-1":720646828776,
"eu-west-1":141502667606,
"eu-west-2":764974769150,
"eu-central-1":492215442770,
"ca-central-1":341280168497,
"us-gov-west-1":414596584902,
}


image="{}.dkr.ecr.{}.amazonaws.com/sagemaker-scikit-learn:0.20.0-cpu-py3".format(aws_sagemaker_region_table_map[region], region)
print(image)


#from awsglue.utils import getResolvedOptions

client = boto3.client('sagemaker')


response = client.create_training_job(
    TrainingJobName=training_job_name,
    HyperParameters={
          'sagemaker_program': entry_point,
          "sagemaker_submit_directory": s3_src_tar_uri
    },
     AlgorithmSpecification={
                    'TrainingImage':image,
                    'TrainingInputMode': 'File'},
    RoleArn=role_arn,
    InputDataConfig=[
        {
            'ChannelName': 'train',
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': s3_train_data_uri,
                    'S3DataDistributionType': 'FullyReplicated'
                }
            },
            'ContentType': 'text/csv',
            'CompressionType': 'None'
        },

    ],
    OutputDataConfig={
        'S3OutputPath':s3_output_model_uri
    },
    ResourceConfig={
        'InstanceType': 'ml.c5.4xlarge',
        'InstanceCount': 1,
        'VolumeSizeInGB': 50
    },
    StoppingCondition={
        'MaxRuntimeInSeconds': 86400
    }
)
print(f"Training Job {training_job_name} has been created..")



# #### Wait for training job to complete


def wait_to_complete_training_job(training_job_name):
        client = boto3.client('sagemaker')
        status = client.describe_training_job(
            TrainingJobName=training_job_name
        )
        print(training_job_name + " job status:", status)
        print("Waiting for " + training_job_name + " training job to complete...")
        
        client.get_waiter('training_job_completed_or_stopped').wait(TrainingJobName=training_job_name)
        resp = client.describe_training_job(TrainingJobName=training_job_name)
        status = resp['TrainingJobStatus']
        
        print("Training job " + training_job_name + " ended with status: " + status)
        if status == 'Failed':
            message = resp['FailureReason']
            print('Training job {} failed with the following error: {}'.format(training_job_name, message))
            raise Exception('Creation of sagemaker Training job failed')
        return status


wait_to_complete_training_job(training_job_name)


# ## Deploy model



model_name= "{}-{}".format(training_job_prefix, datetime.now().strftime("%Y%m%d%H%M%S"))

client = boto3.client('sagemaker')
create_model = client.create_model(
            ModelName= model_name,
            PrimaryContainer=
            {
                'Image': image,
                'ModelDataUrl':'{}/{}/output/model.tar.gz'.format(s3_output_model_uri,training_job_name),
                'Environment' :{'SAGEMAKER_PROGRAM'.upper() : entry_point, 
                                "sagemaker_submit_directory".upper() :s3_src_tar_uri 
                               }
            },
            
            ExecutionRoleArn=role_arn
        )




deploy_instance = "ml.t2.medium"

endpoint_config_name= "{}-{}".format(training_job_prefix, datetime.now().strftime("%Y%m%d%H%M%S"))

client = boto3.client('sagemaker')
create_endpoint_config_api_response = client.create_endpoint_config(
                                            EndpointConfigName=endpoint_config_name,
                                            ProductionVariants=[
                                                {
                                                    'VariantName':'{}-variant-1'.format('endpoint'),
                                                    'ModelName': model_name,
                                                    'InitialInstanceCount': 1,
                                                    'InstanceType':deploy_instance
                                                },
                                            ]
                                        )

print ("create_endpoint_config API response", create_endpoint_config_api_response)


# In[45]:


# create sagemaker endpoint
endpoint_name= "{}-{}".format(training_job_prefix, datetime.now().strftime("%Y%m%d%H%M%S"))


create_endpoint_api_response = client.create_endpoint(EndpointName=endpoint_name, 
                                                      EndpointConfigName=endpoint_config_name)
print ("create_endpoint API response", create_endpoint_api_response)            


# In[50]:


def wait_to_complete_endpoint_creation(endpoint_name):
        client = boto3.client('sagemaker')
        resp = client.describe_endpoint(EndpointName=endpoint_name)
        print(resp)

        print("Waiting....")
        client.get_waiter('endpoint_in_service').wait(EndpointName=endpoint_name)
        resp = client.describe_endpoint(EndpointName=endpoint_name)
        status = resp['EndpointStatus']
        
        if status == 'Failed':
            message = resp['FailureReason']
            print('Endpoint  {} failed with the following error: {}'.format(endpoint_name, message))
            raise Exception('Creation of sagemaker endpoint failed {}'.format(message))
        return status


# In[51]:


wait_to_complete_endpoint_creation(endpoint_name)


# ## Run predictions


#sample_file = "deploy_test.csv"
#sample_file = s3_train_data_uri

# In[53]:


# import pandas as pd
# deploy_test = pd.read_csv(sample_file).values.tolist()


# # In[54]:


# # Format the deploy_test data features
# request_body = ""
# for sample in deploy_test:
#     request_body += ",".join([str(n) for n in sample[1:-1]]) + "|"
# request_body = request_body[:-1]


# # create sagemaker client using boto3
# client = boto3.client('sagemaker-runtime')

# # Specify endpoint and content_type
# #endpoint_name = "demo-sklearn-boto3-20210224025227"
# content_type = "text/csv"


# # In[56]:


# response = client.invoke_endpoint(
#     EndpointName=endpoint_name,
#     ContentType=content_type,
#     Body=request_body
#     )


# # In[57]:


# # Print out expected and returned labels
# print(f"Expected {', '.join([n[-1] for n in deploy_test])}")
# print("Returned:")
# print(response['Body'].read())


# ## Clean up demo endpoint

# In[58]:


#boto3.client('sagemaker').delete_endpoint(EndpointName=endpoint_name)


