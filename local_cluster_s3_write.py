import ray
import os
import json
import boto3
from datetime import datetime

# Get AWS credentials from environment
aws_env_vars = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN"),
    "AWS_REGION": os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
}

# Print environment variables (obscuring secret values)
print("Setting AWS environment variables:")
for key, value in aws_env_vars.items():
    if value:
        if "SECRET" in key or "TOKEN" in key:
            print(f"  {key}: {'*' * 8}")
        else:
            print(f"  {key}: {value[:4]}{'*' * 8}" if key == "AWS_ACCESS_KEY_ID" and value else f"  {key}: {value}")
    else:
        print(f"  {key}: Not set")

# Initialize Ray with AWS credentials in the runtime environment
ray.init(runtime_env={"env_vars": aws_env_vars})
print(ray.cluster_resources())

@ray.remote
def s3_write_file(bucket_name='sagemaker-poc-dev1', base_prefix='rishabh/'):
    """
    Ray task to write a file to an S3 bucket
    """
    import os
    import json
    import boto3
    from datetime import datetime
    
    try:
        # Create a sample file with timestamp
        test_data = {
            "timestamp": datetime.now().isoformat(),
            "message": "Data from Ray job"
        }
        
        # Create a unique filename
        filename = f"file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        local_file_path = f"/tmp/{filename}"
        s3_key = f"{base_prefix}{filename}"
        
        # Write data to local file
        with open(local_file_path, 'w') as f:
            json.dump(test_data, f)
        
        print(f"Created local file: {local_file_path}")
        
        # Create boto3 session with environment variables
        aws_region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION') or 'us-west-2'
        
        # Create S3 client 
        s3_client = boto3.client('s3')
        
        # Upload file to S3
        print(f"Uploading to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        
        return {
            "success": True,
            "bucket": bucket_name,
            "s3_path": f"s3://{bucket_name}/{s3_key}",
            "local_file": local_file_path
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Run the S3 write operation
print("Writing file to S3...")
result = ray.get(s3_write_file.remote())
print(f"Result: {result}")
