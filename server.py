import os
import uuid
import tempfile
import shutil
import logging
from flask import Flask, request, jsonify
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import docker
from docker.errors import ContainerError, ImageNotFound, APIError
#loading env 
from dotenv import load_dotenv
load_dotenv()
# --- Configuration (Best practice: Use environment variables) ---
# Object Store Config
# S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL") # e.g., 'http://minio.example.com:9000'
# S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
# S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
# S3_REGION = os.environ.get("S3_REGION", "us-east-1") # Default region

S3_ACCESS_KEY =  os.environ.get("S3_ACCESS_KEY")

S3_SECRET_KEY =os.environ.get("S3_SECRET_KEY")
S3_REGION =  "eu-north-1"
# Default Function Config (Static for now, as requested)
DEFAULT_BUCKET_NAME = os.environ.get("DEFAULT_BUCKET_NAME", "faas-code") #

DEFAULT_DOCKER_IMAGE = os.environ.get(
    "DEFAULT_DOCKER_IMAGE", "python:3.10-slim"
)
# Assuming the script fetched is the entrypoint
DEFAULT_CONTAINER_COMMAND = ["python", "/app/script.py"]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Initialize Flask App ---
app = Flask(__name__)

# --- Initialize Clients (Error handling for missing config) ---
s3_client = None
docker_client = None

try:
    if not all([ S3_ACCESS_KEY, S3_SECRET_KEY]):
        logging.warning(
            "S3 environment variables (S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY) not fully set. S3 functionality disabled."
        )
    else:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
        )
        logging.info(f"S3 client initialized for endpoint: {'aws'}")

    docker_client = docker.from_env()
    docker_client.ping() # Check docker daemon connection
    logging.info("Docker client initialized successfully.")

except NoCredentialsError:
    logging.error("AWS credentials not found. Please configure S3_ACCESS_KEY and S3_SECRET_KEY.")
    s3_client = None # Ensure client is None if init fails
except Exception as e:
    logging.error(f"Error initializing Docker client: {e}")
    docker_client = None # Ensure client is None if init fails


# --- Helper Function: Download Code ---
def download_code(bucket, key, download_path):
    """Downloads code from S3 to a local path."""
    if not s3_client:
        raise ConnectionError("S3 client is not initialized.")
    try:
        logging.info(f"Attempting to download s3://{bucket}/")
        s3_client.download_file(bucket, key, download_path)
        logging.info(f"Successfully downloaded code to {download_path}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logging.error(f"S3 object not found: s3://{bucket}/{key}")
        else:
            logging.error(f"Error downloading from S3: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during S3 download: {e}")
        return False

# --- Helper Function: Run Code in Container ---
def run_in_container(image, command, code_dir):
    """Runs the specified command in a Docker container with the code mounted."""
    if not docker_client:
        raise ConnectionError("Docker client is not initialized.")

    host_code_path = os.path.abspath(code_dir)
    container_code_path = "/app" # Mount point inside the container
    # Ensure the command uses the correct path inside the container
    # Example: if command is ["python", "script.py"], it becomes ["python", "/app/script.py"]
    # This version assumes DEFAULT_CONTAINER_COMMAND already includes the path

    logging.info(f"Running image '{image}' with command '{command}'")
    logging.info(f"Mounting host path '{host_code_path}' to container path '{container_code_path}'")

    try:
        container = docker_client.containers.run(
            image=image,
            command=command,
            volumes={host_code_path: {"bind": container_code_path, "mode": "ro"}}, # Read-only mount
            working_dir=container_code_path,
            stdout=True,
            stderr=True,
            remove=True,  # Automatically remove container when finished
            detach=False, # Run in foreground and wait
        )
        # container object here is actually the logs (stdout/stderr) when detach=False
        stdout = container.decode("utf-8")
        stderr = "" # With detach=False, stderr seems merged into stdout or not easily separable this way
        logging.info(f"Container executed. Output:\n{stdout}")
        return stdout, stderr, None # No error object if successful

    except ContainerError as e:
        logging.error(f"Container execution error: {e}")
        # e.stderr often contains useful info
        return e.stdout.decode("utf-8") if e.stdout else "", e.stderr.decode("utf-8") if e.stderr else str(e), e
    except ImageNotFound:
        logging.error(f"Docker image not found: {image}")
        return "", f"Docker image not found: {image}", ImageNotFound(image)
    except APIError as e:
        logging.error(f"Docker API error: {e}")
        return "", f"Docker API error: {e}", e
    except Exception as e:
        logging.error(f"An unexpected error occurred during container run: {e}")
        return "", f"Unexpected container run error: {e}", e


# --- API Endpoint ---
@app.route("/run", methods=["GET"])
def run_function():
    if not s3_client or not docker_client:
        return jsonify({"error": "Server not fully initialized (S3 or Docker client missing)"}), 503 # Service Unavailable

    # For this primitive version, use static config
    bucket_name = DEFAULT_BUCKET_NAME
    object_key =  str(request.args.get("KEY")) 
    docker_image = DEFAULT_DOCKER_IMAGE
    container_command = DEFAULT_CONTAINER_COMMAND

    # --- Create a temporary directory for the code ---
    # Unique name to avoid conflicts if multiple requests happen concurrently
    temp_dir = tempfile.mkdtemp(prefix="faas_code_")
    local_code_path = os.path.join(temp_dir, "script.py") # Assuming the downloaded file is the script
    logging.info(f"Created temporary directory: {temp_dir}")

    try:
        # --- 1. Download Code ---
        if not download_code(bucket_name, object_key, local_code_path):
            # Error already logged in download_code
            return jsonify({"error": f"Failed to download code from s3://{bucket_name}/{object_key}"}), 500

        # --- 2. Run Code in Container ---
        stdout, stderr, error = run_in_container(
            docker_image, container_command, temp_dir
        )

        # --- 3. Prepare Response ---
        if error:
            # Error already logged in run_in_container
            return jsonify({
                "error": f"Execution failed: {str(error)}",
                "stdout": stdout,
                "stderr": stderr,
            }), 500
        else:
            return jsonify({
                "message": "Execution successful",
                "stdout": stdout,
                "stderr": stderr, # May be empty
            }), 200

    except ConnectionError as e:
         # Handle cases where clients weren't initialized properly
         logging.error(f"Connection Error: {e}")
         return jsonify({"error": str(e)}), 503 # Service Unavailable
    except Exception as e:
        # Catch-all for unexpected errors during the process
        logging.exception("An unexpected error occurred in /run endpoint")
        return jsonify({"error": f"An internal server error occurred: {str(e)}"}), 500
    finally:
        # --- 4. Cleanup ---
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logging.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logging.error(f"Error cleaning up temporary directory {temp_dir}: {e}")


# --- Run the Server ---
if __name__ == "__main__":
    # Check if clients are initialized before starting
    if not s3_client:
        print("ERROR: S3 Client not initialized. Check config and logs. Server will not start S3 operations.")
    if not docker_client:
        print("ERROR: Docker Client not initialized. Check Docker daemon and logs. Server will not start container operations.")

    # Run Flask dev server (for production, use a proper WSGI server like Gunicorn/uWSGI)
    app.run(host="0.0.0.0", port=5000, debug=False) # Set debug=False for production/VM image:w
