import ray
import os
import json
import boto3
from datetime import datetime

# Load AWS configuration from config file
config_path = "aws_config.json"
try:
    with open(config_path, 'r') as f:
        aws_config = json.load(f)
    print(f"Loaded AWS config from {config_path}")
except Exception as e:
    print(f"Error loading config file: {e}")

# First assume the role to get temporary credentials
sts_client = boto3.client('sts')
response = sts_client.assume_role(
    RoleArn=aws_config["role_arn"],
    RoleSessionName=aws_config["role_session_name"]
)
print(f"Successfully assumed role: {aws_config['role_arn']}")

# Extract credentials from the response
credentials = response['Credentials']

# Create AWS environment variables with the assumed role credentials
aws_env_vars = {
    "AWS_ACCESS_KEY_ID": credentials['AccessKeyId'],
    "AWS_SECRET_ACCESS_KEY": credentials['SecretAccessKey'],
    "AWS_SESSION_TOKEN": credentials['SessionToken'],
    "AWS_REGION": os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
}

# Connect to remote Ray cluster with assumed role credentials
ray.init(
    address="ray://localhost:10001", 
    runtime_env={
        "env_vars": aws_env_vars,
        # "pip": ["boto3"]  # Add boto3 as a dependency if not installed on server
    }
)
print("Connected to remote Ray cluster")
print(ray.cluster_resources())

@ray.remote
def s3_write_file(bucket_name=None, base_prefix=None):
    """
    Ray task to write a file to an S3 bucket using the assumed role credentials
    """
    import os
    import json
    import boto3
    from datetime import datetime
    
    # Load config values, using parameters if provided
    try:
        with open("aws_config.json", 'r') as f:
            config = json.load(f)
    except Exception:
        config = {
            "default_s3_bucket": "sagemaker-poc-dev1",
            "default_s3_prefix": "rishabh/"
        }
    
    # Use provided values or defaults from config
    bucket_name = bucket_name or config.get("default_s3_bucket")
    base_prefix = base_prefix or config.get("default_s3_prefix")
    
    try:
        # No need to assume role again, credentials are already in the environment
        print("Using credentials from environment")
        
        # Create a sample file with timestamp
        test_data = {
            "timestamp": datetime.now().isoformat(),
            "message": "Data from Ray job on remote cluster"
        }
        
        # Create a unique filename
        filename = f"file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        local_file_path = f"/tmp/{filename}"
        s3_key = f"{base_prefix}{filename}"
        
        # Write data to local file
        with open(local_file_path, 'w') as f:
            json.dump(test_data, f)
        
        print(f"Created local file: {local_file_path}")
        
        # Create S3 client using environment credentials (from the assumed role)
        s3_client = boto3.client('s3')
        
        # Upload file to S3
        print(f"Uploading to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        
        return {
            "success": True,
            "bucket": bucket_name,
            "s3_path": f"s3://{bucket_name}/{s3_key}",
            "local_file": local_file_path,
            "running_on": "remote cluster"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Run the S3 write operation
print("Writing file to S3 from remote cluster...")
result = ray.get(s3_write_file.remote())
print(f"Result: {result}") 