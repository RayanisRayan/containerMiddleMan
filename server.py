import os
import uuid
import tempfile
import shutil
import logging
from flask import Flask, request, jsonify
import docker
from docker.errors import ContainerError, ImageNotFound, APIError
from dotenv import load_dotenv
import swiftclient

load_dotenv()

# --- Configuration ---
OS_AUTH_URL = os.environ.get("OS_AUTH_URL")
OS_USERNAME = os.environ.get("OS_USERNAME")
OS_PASSWORD = os.environ.get("OS_PASSWORD")
OS_PROJECT_NAME = os.environ.get("OS_PROJECT_NAME")
OS_USER_DOMAIN_NAME = os.environ.get("OS_USER_DOMAIN_NAME", "Default")
OS_PROJECT_DOMAIN_NAME = os.environ.get("OS_PROJECT_DOMAIN_NAME", "Default")
DEFAULT_CONTAINER_NAME = os.environ.get("DEFAULT_CONTAINER_NAME", "faas-code")
DEFAULT_DOCKER_IMAGE = os.environ.get("DEFAULT_DOCKER_IMAGE", "python:3.10-slim")
DEFAULT_CONTAINER_COMMAND = ["python", "/app/script.py"]

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)

swift_conn = None
docker_client = None

try:
    if not all([OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, OS_PROJECT_NAME]):
        logging.warning(
            "Swift environment variables not fully set. Swift functionality disabled."
        )
    else:
        swift_conn = swiftclient.Connection(
            authurl=OS_AUTH_URL,
            user=OS_USERNAME,
            key=OS_PASSWORD,
            tenant_name=OS_PROJECT_NAME,
            auth_version="3",
            os_options={
                "user_domain_name": OS_USER_DOMAIN_NAME,
                "project_domain_name": OS_PROJECT_DOMAIN_NAME,
            },
        )
        logging.info(f"Swift client initialized for endpoint: {OS_AUTH_URL}")

    docker_client = docker.from_env()
    docker_client.ping()
    logging.info("Docker client initialized successfully.")

except Exception as e:
    logging.error(f"Error initializing clients: {e}")
    swift_conn = None
    docker_client = None

# --- Helper Function: Download Code from Swift ---
def download_code(container, object_name, download_path):
    """Downloads code from Swift to a local path."""
    if not swift_conn:
        raise ConnectionError("Swift client is not initialized.")
    try:
        logging.info(f"Attempting to download swift://{container}/{object_name}")
        headers, obj_contents = swift_conn.get_object(container, object_name)
        with open(download_path, "wb") as f:
            f.write(obj_contents)
        logging.info(f"Successfully downloaded code to {download_path}")
        return True
    except swiftclient.exceptions.ClientException as e:
        if e.http_status == 404:
            logging.error(f"Swift object not found: swift://{container}/{object_name}")
        else:
            logging.error(f"Error downloading from Swift: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during Swift download: {e}")
        return False

# --- Helper Function: Run Code in Container (unchanged) ---
def run_in_container(image, command, code_dir):
    if not docker_client:
        raise ConnectionError("Docker client is not initialized.")

    host_code_path = os.path.abspath(code_dir)
    container_code_path = "/app"
    logging.info(
        f"Running image '{image}' with command '{command}'"
    )
    logging.info(
        f"Mounting host path '{host_code_path}' to container path '{container_code_path}'"
    )

    try:
        container = docker_client.containers.run(
            image=image,
            command=command,
            volumes={host_code_path: {"bind": container_code_path, "mode": "ro"}},
            working_dir=container_code_path,
            stdout=True,
            stderr=True,
            remove=True,
            detach=False,
        )
        stdout = container.decode("utf-8")
        stderr = ""
        logging.info(f"Container executed. Output:\n{stdout}")
        return stdout, stderr, None

    except ContainerError as e:
        logging.error(f"Container execution error: {e}")
        return (
            e.stdout.decode("utf-8") if e.stdout else "",
            e.stderr.decode("utf-8") if e.stderr else str(e),
            e,
        )
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
    if not swift_conn or not docker_client:
        return (
            jsonify(
                {
                    "error": "Server not fully initialized (Swift or Docker client missing)"
                }
            ),
            503,
        )

    container_name = DEFAULT_CONTAINER_NAME
    object_key = str(request.args.get("KEY"))
    docker_image = DEFAULT_DOCKER_IMAGE
    container_command = DEFAULT_CONTAINER_COMMAND

    temp_dir = tempfile.mkdtemp(prefix="faas_code_")
    local_code_path = os.path.join(temp_dir, "script.py")
    logging.info(f"Created temporary directory: {temp_dir}")

    try:
        if not download_code(container_name, object_key, local_code_path):
            return (
                jsonify(
                    {
                        "error": f"Failed to download code from swift://{container_name}/{object_key}"
                    }
                ),
                500,
            )

        stdout, stderr, error = run_in_container(
            docker_image, container_command, temp_dir
        )

        if error:
            return (
                jsonify(
                    {
                        "error": f"Execution failed: {str(error)}",
                        "stdout": stdout,
                        "stderr": stderr,
                    }
                ),
                500,
            )
        else:
            return (
                jsonify(
                    {
                        "message": "Execution successful",
                        "stdout": stdout,
                        "stderr": stderr,
                    }
                ),
                200,
            )

    except ConnectionError as e:
        logging.error(f"Connection Error: {e}")
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        logging.exception("An unexpected error occurred in /run endpoint")
        return (
            jsonify({"error": f"An internal server error occurred: {str(e)}"}),
            500,
        )
    finally:
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logging.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logging.error(
                    f"Error cleaning up temporary directory {temp_dir}: {e}"
                )
@app.route("/upload", methods=["POST"])
def upload_python_file():
    if not swift_conn:
        return (
            jsonify({"error": "Swift client not initialized."}),
            503,
        )

    # Check if the request has the file part
    if "file" not in request.files:
        return jsonify({"error": "No file part in the request."}), 400

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"error": "No selected file."}), 400

    # Only allow .py files
    if not file.filename.endswith(".py"):
        return jsonify({"error": "Only .py files are allowed."}), 400

    # Generate a unique object key (or use the filename)
    object_key = f"{uuid.uuid4().hex}_{file.filename}"

    try:
        # Read file content
        file_content = file.read()

        # Upload to Swift
        swift_conn.put_object(
            container=DEFAULT_CONTAINER_NAME,
            obj=object_key,
            contents=file_content,
            content_type="text/x-python"
        )

        logging.info(
            f"Uploaded file '{file.filename}' as '{object_key}' to Swift container '{DEFAULT_CONTAINER_NAME}'"
        )

        return (
            jsonify(
                {
                    "message": "File uploaded successfully.",
                    "object_key": object_key,
                    "container": DEFAULT_CONTAINER_NAME,
                }
            ),
            201,
        )

    except Exception as e:
        logging.error(f"Error uploading file to Swift: {e}")
        return jsonify({"error": f"Failed to upload file: {str(e)}"}), 500
@app.route("/list-objects", methods=["GET"])
def list_objects():
    if not swift_conn:
        return jsonify({"error": "Swift client not initialized."}), 503
    try:
        # List all objects in the default container
        objects = swift_conn.get_container(DEFAULT_CONTAINER_NAME)[1]
        object_keys = [obj["name"] for obj in objects]
        return jsonify({"object_keys": object_keys}), 200
    except Exception as e:
        logging.error(f"Error listing objects in Swift: {e}")
        return jsonify({"error": f"Failed to list objects: {str(e)}"}), 500

if __name__ == "__main__":
    if not swift_conn:
        print(
            "ERROR: Swift Client not initialized. Check config and logs. Server will not start Swift operations."
        )
    if not docker_client:
        print(
            "ERROR: Docker Client not initialized. Check Docker daemon and logs. Server will not start container operations."
        )

    app.run(host="0.0.0.0", port=5000, debug=False)
