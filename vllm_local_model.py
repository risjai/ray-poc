import os
import json
import time
import shutil
from pathlib import Path
from huggingface_hub import snapshot_download
from vllm import LLM, SamplingParams

# Load AWS configuration if needed
# aws_config_path = "aws_config.json"
# aws_config = {}
# if os.path.exists(aws_config_path):
#     with open(aws_config_path, "r") as f:
#         aws_config = json.load(f)

# Define a small model to load - GPT-2 is a good lightweight option
MODEL_NAME = "gpt2"  # 124M parameters
LOCAL_MODEL_PATH = Path("./local_models") / MODEL_NAME.replace("/", "_")

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

# Initialize vLLM with the local model
print(f"Loading model from local path: {local_path}")
llm = LLM(
    model=str(local_path),  # Use the local path
    trust_remote_code=True,
    dtype="auto",  # Use auto to let vLLM decide appropriate dtype
    max_model_len=1024,  # Lower for less memory usage
)

# Set up sampling parameters
sampling_params = SamplingParams(
    temperature=0.7,
    top_p=0.95,
    max_tokens=100,
)

def generate_response(prompt):
    """Generate a response from the model."""
    try:
        start_time = time.time()
        outputs = llm.generate(prompt, sampling_params)
        end_time = time.time()
        
        # Get the generated text
        generated_text = outputs[0].outputs[0].text
        time_taken = end_time - start_time
        
        return generated_text, time_taken
    except Exception as e:
        print(f"Error during inference: {str(e)}")
        return str(e), 0

# Test the model with a simple prompt
def test_model():
    print(f"\nTesting locally stored model with a sample prompt")
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
    print("Application has been shut down.") 