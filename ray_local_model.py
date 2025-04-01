import os
import json
import time
import shutil
import ray
from pathlib import Path
from huggingface_hub import snapshot_download
from vllm import LLM, SamplingParams


# Define a small model to load - GPT-2 is a good lightweight option
MODEL_NAME = "gpt2"  # 124M parameters
LOCAL_MODEL_PATH = Path("./local_models_ray/") / MODEL_NAME.replace("/", "_")

def download_model(model_name, save_path):
    """Download model from Hugging Face and save locally."""
    print(f"Downloading model {model_name} to {save_path}...")
    
    # Create the directory if it doesn't exist
    os.makedirs(save_path, exist_ok=True)
    
    # Check if the model is already downloaded
    if os.path.exists(os.path.join(save_path, "config.json")):
        print(f"Model already exists at {save_path}")
        return save_path
    
    # Download the model
    try:
        snapshot_download(
            repo_id=model_name,
            local_dir=save_path,
            ignore_patterns=["*.msgpack", "*.h5", "*.safetensors", "optimizer.pt"],  # Exclude large files not needed
        )
        print(f"Model downloaded successfully to {save_path}")
        return save_path
    except Exception as e:
        print(f"Error downloading model: {str(e)}")
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
        raise

# Download the model first
print(f"Preparing to download model: {MODEL_NAME}")
local_path = download_model(MODEL_NAME, LOCAL_MODEL_PATH)
print(f"Model stored at {local_path}")
# local_path ='local_models_ray/gpt2'

# Initialize Ray
print("Starting Ray cluster...")
ray.init()
print("Ray cluster initialized")
print("Available resources:", ray.cluster_resources())

# Define the remote model inference class
@ray.remote
class ModelInferenceService:
    def __init__(self, model_path):
        self.model_path = model_path
        print(f"Initializing vLLM with model from {model_path}")
        self.llm = LLM(
            model=str(model_path),  # Use the local path
            trust_remote_code=True,
            dtype="auto",  # Use auto to let vLLM decide appropriate dtype
            max_model_len=1024,  # Lower for less memory usage
        )
        self.sampling_params = SamplingParams(
            temperature=0.7,
            top_p=0.95,
            max_tokens=100,
        )
        print("Model loaded successfully")

    def generate(self, prompt):
        try:
            outputs = self.llm.generate(prompt, self.sampling_params)
            return outputs[0].outputs[0].text
        except Exception as e:
            return f"Error during inference: {str(e)}"

# Create a remote model service
print("Creating remote model service...")
model_service = ModelInferenceService.remote(local_path)

def generate_response(prompt):
    """Generate a response from the model using Ray."""
    try:
        start_time = time.time()
        result = ray.get(model_service.generate.remote(prompt))
        end_time = time.time()
        return result, end_time - start_time
    except Exception as e:
        print(f"Error during Ray call: {str(e)}")
        return str(e), 0

# Test the model with a simple prompt
def test_model():
    print(f"\nTesting locally stored model with Ray")
    prompt = "Tell me a short joke about"
    print(f"Prompt: {prompt}")
    
    response, time_taken = generate_response(prompt)
    print(f"Response: {response}")
    print(f"Time taken: {time_taken:.2f} seconds")

# Run the test
test_model()

# Interactive loop
print("\nModel is loaded and running. Press Ctrl+C to exit.")
try:
    while True:
        prompt = input("\nEnter a prompt (or 'exit' to quit): ")
        if prompt.lower() == 'exit':
            break
        
        response, time_taken = generate_response(prompt)
        print(f"Response: {response}")
        print(f"Time taken: {time_taken:.2f} seconds")
except KeyboardInterrupt:
    print("\nShutting down...")
finally:
    # Clean up
    ray.shutdown()
    print("Ray has been shut down.") 