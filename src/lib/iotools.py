from pyspark.sql import SparkSession
from urllib.parse import urlparse
import json
from pyspark.sql import functions as F
import pandas as pd
import shutil



spark = SparkSession.builder.getOrCreate()
try:
    dbutils  
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)  

def is_s3_path(bucket: str, path: str) -> bool:
    """
    Check if there is at least one object under the given S3 prefix.
    """
    if not bucket or not path:
        return False

    hard_prefix = path if path.endswith("/") else path + "/"
    full_prefix = f"s3://{bucket}/{hard_prefix}"

    # Fast attempt: try listing with dbutils.fs.ls
    try:
        entries = dbutils.fs.ls(full_prefix)
        if entries:
            return True
    except Exception:
        # fall through to Spark approach
        pass

    # Serverless-safe Spark listing (recursive)
    try:
        df = (
            spark.read.format("binaryFile")  # noqa: F821
            .option("recursiveFileLookup", "true")
            .load(full_prefix)
            .select(F.col("path"))
            .limit(1)
        )
        return df.count() > 0
    except Exception:
        return False
    
def is_s3_file(bucket: str, key: str) -> bool:
    """
    Check if an exact S3 object exists.
    """
    if not bucket or not key:
        return False
    file_path = f"s3://{bucket}/{key}"
    try:
        df = (
            spark.read.format("binaryFile")  # noqa: F821
            .load(file_path)
            .select(F.col("path"))
            .limit(1)
        )
        return df.count() > 0
    except Exception:
        return False

def split_path_bucket_key(path):
    """Split a full path into a bucket and key for s3 writes.

    Does what it says.

    Parameters:
        path (str): full s3 path
    Returns:
        bucket (str): bucket portion of s3 path
        key (str): key portion of s3 path
    """
    parsed = urlparse(path)
    bucket = parsed.netloc
    key = parsed.path[1:]
    return (bucket, key)

def load_json_as_dict(path: str, encoding: str = "UTF-8") -> dict:
    # Reads exactly that file; no RDD, no boto3
    df = (spark.read.format("binaryFile")
          .load(path)
          .select(F.decode(F.col("content"), encoding).alias("json_str")))
    json_str = df.first()["json_str"]  # small config files only
    return json.loads(json_str)

def write_json_to_s3(path, data, vol_base = "/Volumes/datascience_ea_dev/pe/outputs_for_s3", env="dev"):
    vol_path =  vol_base + urlparse(path).path
    dbutils.fs.mkdirs('/'.join(vol_path.split('/')[:-1]))
    json_content = json.dumps(data, indent=2, sort_keys=True)
    with open(vol_path, "w") as f:
        f.write(json_content)
    if env not in ["dev", "qa"]:
        dbutils.fs.mv(vol_path, path)

def write_local_to_s3(data, path, mode="overwrite", vol_base = "/Volumes/datascience_ea_dev/pe/outputs_for_s3", env="dev"):
    """Write local data to s3.

    Writes local data to s3. Will choose output types based on
    datatype of data to be written, either writing csv/txt or
    binary file.

    Parameters:
        data (?): data to write out to s3
        path (str): s3 path to write data to
        mode(str, opt): how to handle existing file, default is overwrite
    Returns:
        Nothing!
    """
    # if "pd" not in vars() and "pd" not in globals():
    #     import pandas as pd
    # if "joblib" not in vars() and "joblib" not in globals():
    #     import joblib

    vol_path =  vol_base + urlparse(path).path
    dbutils.fs.mkdirs('/'.join(vol_path.split('/')[:-1]))

    # if isinstance(data, dict) | isinstance(data, pd.DataFrame):
    if isinstance(data, dict):
        # make dataframe
        file = pd.DataFrame.from_dict([data])
    else:
        file = data
    file.to_csv(vol_path, index=False)
    if env not in ["dev", "qa"]:
        dbutils.fs.mv(vol_path, path)

def read_s3_to_local(path, ftype="csv", columns=None):
    if ftype=="csv":
        data = spark.read.csv(path, header="true", inferSchema=True).toPandas()
        if columns:
            data.columns = columns
    else:
        raise Exception("Not implemented yet")
    return data

        
    # if mode.lower() == "append":
    #     try:
    #         ftype = re.findall(r"[\.]([a-zA-Z]{1,8})$", path)[0]
    #         oldfile = read_s3_to_local(path, ftype=ftype)
    #         combined = pd.concat([oldfile, file], ignore_index=True)
    #     except:
    #         combined = file
    #         print("couldn't read existing file")
    #     file = combined
    # else:
    #     pass
    # write
    # if sys.version_info > (3, 0):
    #     csv_buffer = io.StringIO()
    # else:
    #     csv_buffer = io.BytesIO()
    
    # file.to_csv(csv_buffer, index=False)

    # s3_resource = boto3.resource("s3")
    # if encryption is None:
    #     s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
    # else:
    #     s3_resource.Object(bucket, key).put(
    #         Body=csv_buffer.getvalue(), ServerSideEncryption=encryption
    #     )
    # else:

    #     my_mock_file = mock_file(mode="wb+")
    #     joblib.dump(data, my_mock_file)
    #     data_serialized = my_mock_file.read()

    #     s3 = boto3.resource("s3")
    #     if encryption is None:
    #         dump_s3 = (
    #             lambda f, data: s3.Bucket(bucket)
    #             .Object(key=f)
    #             .put(Body=data_serialized)
    #         )
    #     else:
    #         dump_s3 = (
    #             lambda f, data: s3.Bucket(bucket)
    #             .Object(key=f)
    #             .put(Body=data_serialized, ServerSideEncryption=encryption)
    #         )
    #     try:
    #         s3.Object(bucket, key).load()
    #     except ClientError as e:
    #         if e.response["Error"]["Code"] == "404":
    #             dump_s3(key, data_serialized)
    #     else:
    #         s3.Bucket(bucket).Object(key=key).delete()
    #         dump_s3(key, data_serialized)

    return

def copy_file_to_s3(local_path, s3_path, vol_base = "/Volumes/datascience_ea_dev/pe/outputs_for_s3", env="dev"):
    """
    Copy the local file from local_path to s3_path.

    Parameters:
        local_path (str): path for the local source file
        s3_path (str): path for the S3 destination file

    Returns: None
    """
    vol_path =  vol_base + urlparse(s3_path).path
    dbutils.fs.mkdirs('/'.join(vol_path.split('/')[:-1]))

    shutil.copy(local_path, vol_path)
    if env not in ["dev", "qa"]:
        dbutils.fs.mv(vol_path, s3_path)