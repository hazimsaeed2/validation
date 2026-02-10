import os
import subprocess
import sys
import shutil
import urllib.request
import zipfile
import stat

# --- CONFIGURATION ---
# We install to /tmp to ensure it works on any cluster without permission issues
CLI_ROOT = "/tmp/dab-cli-portable"
CLI_EXEC_PATH = os.path.join(CLI_ROOT, "databricks")
# Pinning a version is safer for production stability than "latest"
CLI_VERSION = "0.286.0" 
CLI_DOWNLOAD_URL = f"https://github.com/databricks/cli/releases/download/v{CLI_VERSION}/databricks_cli_{CLI_VERSION}_linux_amd64.zip"

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

try:
    dbutils
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)


def install_cli_portable():
    """
    Downloads and unzips the Databricks CLI manually.
    This bypasses the install script to avoid path collisions on clusters.
    """
    if os.path.exists(CLI_EXEC_PATH):
        print(f"✅ CLI already exists at {CLI_EXEC_PATH}")
        return

    print(f"⬇️  Downloading Databricks CLI v{CLI_VERSION}...")
    
    # 1. Clean up any partial state
    if os.path.exists(CLI_ROOT):
        shutil.rmtree(CLI_ROOT)
    os.makedirs(CLI_ROOT, exist_ok=True)

    # 2. Download ZIP
    zip_path = os.path.join(CLI_ROOT, "databricks.zip")
    try:
        urllib.request.urlretrieve(CLI_DOWNLOAD_URL, zip_path)
    except Exception as e:
        print(f"❌ Failed to download CLI from {CLI_DOWNLOAD_URL}")
        raise e

    # 3. Unzip
    print("📦 Extracting...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(CLI_ROOT)

    # 4. Make executable (chmod +x)
    st = os.stat(CLI_EXEC_PATH)
    os.chmod(CLI_EXEC_PATH, st.st_mode | stat.S_IEXEC)
    
    print(f"✅ Installed standalone CLI to: {CLI_EXEC_PATH}")

def deploy_bundle(host, client_id, client_secret, bundle_path, target="dev"):
    # 1. Install CLI if missing
    install_cli_portable()

    # 2. Set Env Vars for SP Auth
    env_vars = os.environ.copy()
    env_vars["DATABRICKS_HOST"] = host
    env_vars["DATABRICKS_CLIENT_ID"] = client_id
    env_vars["DATABRICKS_CLIENT_SECRET"] = client_secret
    
    # 3. Run Deploy
    cmd = [
        CLI_EXEC_PATH, 
        "bundle", 
        "deploy", 
        "--target", target
    ]

    print(f"🚀 Starting deployment to target: '{target}'...")
    print(f"📂 Bundle Path: {bundle_path}")

    try:
        process = subprocess.run(
            cmd,
            cwd=bundle_path,
            env=env_vars,
            capture_output=True,
            text=True,
            check=True 
        )
        print("\n✅ Deployment Successful!")
        print(process.stdout)
        
    except subprocess.CalledProcessError as e:
        print("\n❌ Deployment Failed!")
        print("Standard Output:", e.stdout)
        print("Error Output:", e.stderr)
        raise e
if __name__ == "__main__":

    dbutils.widgets.text("host", "https://dbc-ad8c0ca0-6db0.cloud.databricks.com")
    dbutils.widgets.text("client_id", "ce0f4d1c-421f-45d6-8169-244653bc84d7")
    dbutils.widgets.text("client_secret", "")
    dbutils.widgets.text("root_path", ".")
    dbutils.widgets.text("target", "dev")

    host = dbutils.widgets.get("host")
    client_id = dbutils.widgets.get("client_id")
    client_secret = dbutils.widgets.get("client_secret")
    root_path = os.getcwd()#dbutils.widgets.get("root_path")
    target = dbutils.widgets.get("target")

    # parser = argparse.ArgumentParser(description="Deploy Databricks Asset Bundle using Service Principal")
    
    # parser.add_argument("--host", required=True, help="Databricks Workspace URL (e.g., https://dbc-xxxx.com)")
    # parser.add_argument("--client_id", required=True, help="Service Principal Client/Application ID")
    # parser.add_argument("--client_secret", required=True, help="Service Principal Client Secret")
    # parser.add_argument("--root_path", default=".", help="Path to the root of the bundle (directory containing databricks.yml)")
    # parser.add_argument("--target", default="dev", help="Target environment (dev, prod, staging)")

    # args = parser.parse_args()

    # check_cli_installed()
    deploy_bundle(
        host=host, 
        client_id=client_id, 
        client_secret=client_secret, 
        bundle_path=root_path,
        target=target
    )