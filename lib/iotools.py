"""Shared I/O utility functions.

TODO: (r/w local) - Handle case where partitions are in subfolders -- folder_name/date_partition_folder_name/date_partition.parquet
      (r/w local) - Read in .dat file (csv) by improving key selection methodology to handle ambiguous extensions
      (r/w local) - More informative error message when the file type in not accepted

"""

from datetime import datetime
import collections
import io
import json
import os
import re
import sys
from urllib.parse import urlparse
import warnings

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dateutil import rrule
import yaml

from pe_memberdna.lib.utils import next_fiscal_week_end

ALLOWED_DELETE_PREFIXES = [
    re.compile("USERDATA/[^()]"),
    re.compile(r"REGRESSION_TESTS/.*/\<.*(>)"),
]

# ------ HELPERS ----- #


class mock_file:
    """
    This is a file-like object:
    A class that pretends to be a file but really stores everyting in a local
    bytes variable.

    Note that not all methods of the built-in class file are implemented since
    they were not neccessary. If you need more please feel free to implement
    them. The ones implemented are the minimum needed to get joblib.dump
    and joblib.load to work.

    Use the following link to implement more methods:
    https://docs.python.org/2.4/lib/bltin-file-objects.html
    """

    def __init__(self, mode="wb+"):
        """
        Only wb+ mode is implemented for now
        """
        self.temp = b""
        self.pointer = 0

    def write(self, str_in):
        """
        Simple write
        """
        self.temp += str_in

    def seek(self, offset=0):
        """
        Sets pointer position
        """
        self.pointer = offset

    def readline(self):
        """
        Reads data between the pointer and the first new line character
        inclusing the new line character
        """
        pointer_new = self.temp.index(b"\n", self.pointer) + 1
        res = self.temp[self.pointer : pointer_new]
        self.pointer = pointer_new
        return res

    def read(self, n_bytes=None):
        """
        When n_bytes is none returns everyting between the pointer and the EOF
        When n_bytes is a number returns n_bytes bytes immediatelly following
        the pointer
        """
        if n_bytes:
            res = self.temp[self.pointer : (self.pointer + n_bytes)]
            self.pointer += n_bytes
        else:
            res = self.temp
            self.pointer = len(self.temp)

        return res


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


def filter_time_partition(
    paths, start_time, end_time, partition_range="daily"
):
    """Filter a time partitioned file for a specific time range.

    We store most intermediate data in parquet format partitioned by date.
    This function allows filtering of a list of date partitioned paths for a given time range.
    It can handle different partition sizes (daily,weekly, monthly).

    Parameters:
        paths (list(str)): list of paths to filter
        start_time (str): start of date partitioned parquet file range in 'YYYY-MM-dd' format
        end_time (str): end of date partitioned parquet file range in 'YYYY-MM-dd' format
        partition_range (str): time interval of date partitioning, currently supports 'daily' 'weekly'
        and 'monthly'
    Returns:
        paths (list(str)): filtered list of relevant paths
    """
    if partition_range == "daily":
        rule = rrule.DAILY
    elif partition_range == "weekly":
        rule = rrule.WEEKLY
        start_time = next_fiscal_week_end(start_time)
    elif partition_range == "monthly":
        rule = rrule.MONTHLY
    else:
        raise ValueError(
            "only daily, weekly, and monthly partitions are supported"
        )
    dates = rrule.rrule(
        rule,
        dtstart=datetime.strptime(start_time, "%Y-%m-%d"),
        until=datetime.strptime(end_time, "%Y-%m-%d"),
    )
    return [
        path
        for date in dates
        for path in paths
        if "={}".format(date.strftime("%Y-%m-%d")) in path
    ]


# ------ STANDARD API ----- #
def get_filename(path):
    """Extract file name from the given path."""
    return path.split("/")[-1]


def is_s3_path(bucket, path):
    """
        Check S3 objects to see if there is at least one that has the path
        as its prefix(parent dir). Append "/" if needed to force dir ending.

    Parameters:
        bucket: string representing the s3 bucket
        path: string representing a path

    Returns:
         boolean
    """

    config = Config(connect_timeout=5)
    s3 = boto3.resource("s3", config=config)
    bucket_obj = s3.Bucket(bucket)

    hard_prefix = path if path.endswith("/") else path + "/"

    return (
        len(list(bucket_obj.objects.filter(Prefix=hard_prefix).limit(1))) == 1
    )


def is_s3_file(bucket, key):
    """
        Try to fetch s3 object having the specified key.

    Parameters:
        bucket: string representing the s3 bucket
        key: string representing a file path
    Returns:
         boolean
    """

    if not bucket or not key:
        return False

    config = Config(connect_timeout=5)
    s3 = boto3.client("s3", config=config)

    try:
        s3.get_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def list_s3_dir(bucket, path):
    """
        Lists all S3 objects which are inside this path. Similar to listing
        files in a directory. This function forces path to end in "/".
        IMPORTANT: Directories will not be listed. They are not S3 objects, but
        all objects from child directories will be listed.

    Parameters:
        bucket: string representing the s3 bucket
        path: string prefix
    Returns:
        all_keys([str, str,...]): a list containing full S3 uri
    """

    hard_prefix = path if path.endswith("/") else path + "/"
    return list_s3_files(bucket, hard_prefix)


def list_s3_files(bucket, prefix):
    """
        Lists all S3 objects which start with this prefix.
        IMPORTANT: Directories will not be listed. They are not S3 objects, but
        all objects from child directories will be listed.

    Parameters:
        bucket: string representing the s3 bucket
        prefix: string prefix
    Returns:
        all_keys([str, str,...]): a list containing full S3 uri
    """
    config = Config(connect_timeout=5)
    s3 = boto3.resource("s3", config=config)

    bucket_obj = s3.Bucket(bucket)

    all_keys = [
        "s3://{bucket}/{key}".format(
            bucket=summary.bucket_name, key=summary.key
        )
        for summary in bucket_obj.objects.filter(Prefix=prefix)
    ]
    return all_keys


def s3_copy_version(bucket, source_key, destination_key, version_id=None):
    """
    Copy an S3 object, optionally with a specific version.

    Note that AWS CLI is avoided so as to not mask when the operation is
    fully completed. (AWS CLI only says when the command is received.)

    Parameters:
        bucket: string representing the s3 bucket
        source_key: string representing a file path
        destination_key: string representing a file path
        version_id: string representing the version to be copied(None for
            current version).
    Returns:
         None
    """
    config = Config(connect_timeout=5)
    s3 = boto3.client("s3", config=config)

    if not is_s3_file(bucket, source_key):
        raise Exception("Can only copy files(s3 keys).")

    source = {"Bucket": bucket, "Key": source_key}
    if version_id:
        source["VersionId"] = version_id

    s3.copy_object(Bucket=bucket, Key=destination_key, CopySource=source)


def s3_copy(bucket, source_key, destination_key):
    """
    Copy an S3 object, or directory recursively, to another location.

    Note that AWS CLI is avoided so as to not mask when the operation is
    fully completed. (AWS CLI only says when the command is received.)

    Parameters:
        bucket: string representing the s3 bucket
        source_key: string representing a file or directory path
        destination_key: string representing a file or directory path
    Returns:
         None
    """

    if is_s3_file(bucket, source_key):
        s3_copy_version(bucket, source_key, destination_key)
    else:
        dir_objs = list_s3_dir(bucket, source_key)
        for obj in dir_objs:
            _, subkey = split_path_bucket_key(obj)

            suffix = subkey[len(source_key) :]
            destination = "{}{}".format(destination_key, suffix)

            s3_copy(bucket, subkey, destination)


def s3_copy_managed(
    bucket_src,
    source_key,
    destination_key,
    bucket_dest=None,
    managed=True,
):
    """
    Copy a s3 file, or directory recursively, to another location.

    Note that AWS CLI is avoided so as to not mask when the operation is
    fully completed. (AWS CLI only says when the command is received.)

    Parameters:
        bucket_src (str): string representing the s3 bucket source
        source_key (str): string representing a source file or directory path
        destination_key (str): string representing a destination file or directory path
        bucket_dest (str): string representing the s3 bucket destination
        managed (bool): if True, use a managed transfer (for files >5Gb)

        Please Note: if encryption type/algorithm is specified then SSEKMSKeyId
        needs to be specified, otherwise AccessDenied excpetion will be thrown.
    Returns:
        None
    """
    config = Config(connect_timeout=5)
    s3 = boto3.client("s3", config=config)

    kwargs = {}

    if not bucket_dest:
        bucket_dest = bucket_src

    if destination_key.endswith("/") and not source_key.endswith("/"):
        # tail of the file was not specified
        destination_key = os.path.join(
            destination_key, os.path.split(source_key)[1]
        )

    if is_s3_file(bucket_src, source_key):
        if not managed:
            s3.copy_object(
                Bucket=bucket_dest,
                Key=destination_key,
                CopySource={"Bucket": bucket_src, "Key": source_key},
                **kwargs
            )
        else:
            s3.copy(
                Bucket=bucket_dest,
                Key=destination_key,
                CopySource={"Bucket": bucket_src, "Key": source_key},
                ExtraArgs=kwargs,
            )
    else:
        dir_objs = list_s3_dir(bucket_src, source_key)
        for obj in dir_objs:
            _, subkey = split_path_bucket_key(obj)

            suffix = subkey[len(source_key) :]
            destination = "{}{}".format(destination_key, suffix)

            s3_copy_managed(
                bucket_src, subkey, destination, bucket_dest, managed
            )


def s3_delete(bucket, key, allowed_paths=None):
    """
    Delete an S3 object, or directory recursively.

    Parameters:
        bucket (str): s3 bucket name to delete from
        key (str): s3 key to delete
        allowed_paths ([str, re.compile]): paths that are allowed to be deleted.
            A string represents an allowed prefix, requiring the key to be at
            least one level deeper to be allowed for deletion; otherwise a
            re.compile() obj may be passed, representing a full required match.

    Returns:
         None
    """
    allowed_paths = allowed_paths or ALLOWED_DELETE_PREFIXES
    allowed, allowed_output = delete_allowed(key, allowed_paths)
    if not allowed:
        raise Exception(allowed_output)

    config = Config(connect_timeout=5)
    s3 = boto3.resource("s3", config=config)

    if is_s3_file(bucket, key):
        s3.Object(bucket, key).delete()
    else:
        dir_objs = list_s3_dir(bucket, key)
        for obj in dir_objs:
            _, subkey = split_path_bucket_key(obj)
            s3_delete(bucket, subkey, allowed_paths)


def delete_allowed(key, allowed_paths=None):
    """
    Identify whether the given key is allowed to be deleted.

    Parameters:
        key (str): s3 key to check for deletion
        allowed_paths ([str, re.compile]): paths that are allowed to be deleted.
            A string represents an allowed prefix; otherwise a
            re.compile() obj may be passed, representing a full required match.

    Returns: (Bool, str/re.compile)
        True, allowed_path
        False, str message explaining that key was not in allowed_paths
    """
    allowed_paths = allowed_paths or ALLOWED_DELETE_PREFIXES
    for allowed_path in allowed_paths:
        if isinstance(allowed_path, str):
            if key.startswith(allowed_path):
                return (True, allowed_path)
        else:  # assume re.compiled object
            matches = re.match(allowed_path, key)
            if matches:
                return (True, allowed_path.pattern)

    unmatched_patterns = [
        p if isinstance(p, str) else p.pattern for p in allowed_paths
    ]
    msg = "Cannot delete {}. Key must use one of the allowed prefixes: {}".format(
        key, unmatched_patterns
    )
    return (False, msg)


def read_s3_to_local(
    path,
    ftype="csv",
    columns=None,
    sep=",",
    start_time=None,
    end_time=None,
    partition_range="daily",
    pub_key="",
    private_key="",
):
    """Read into pandas from s3.

    Read in a file from s3 to local pandas. Leverages Boto to do so. Currently
    supports txt, csv, parquet, json, binary.

    Parameters:
        path (str): s3 path
        type (str): filetype of path(s) represented in "path" arg
        columns(list(str), opt): subset of columns to read. Only works for parquet format
        sep(str, opt): separator for csv types (default ",")
        start_time (str, opt): start of date partitioned file range in 'YYYY-MM-dd' format
        end_time (str, opt): end of date partitioned file range in 'YYYY-MM-dd' format
        partition_range (str, opt): time interval of date partitioning, currently supports 'daily' 'weekly'
        and 'monthly'
    Returns:
        data (pandas.Dataframe): dataframe representation of csv
    """
    if "pd" not in vars() and "pd" not in globals():
        import pandas as pd
    if "joblib" not in vars() and "joblib" not in globals():
        import joblib
    if "pq" not in vars() and "pq" not in globals():
        try:
            import pyarrow.parquet as pq
        except:
            warnings.warn(
                "WARNING: local parquet reads not supported on this kernel"
            )

    config = Config(connect_timeout=5)
    s3 = boto3.client("s3", config=config)
    bucket, bkey = split_path_bucket_key(path)
    # handle case of partitioned OR single file to generate final key list
    objects_dict = s3.list_objects_v2(Bucket=bucket, Prefix=bkey)
    if "Contents" in list(objects_dict.keys()):
        s3_keys = [
            item["Key"]
            for item in objects_dict["Contents"]
            if item["Key"].endswith(ftype)
        ]
    else:
        s3_keys = [bkey]
    # subset by time if appropriate
    if (start_time is not None) & (end_time is not None):
        s3_keys = filter_time_partition(
            s3_keys, start_time, end_time, partition_range
        )
    # read in all keys
    if len(s3_keys) < 1:
        raise ValueError("No Keys to read!")
    dfs = []
    data = None
    for key in s3_keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        if ftype == "parquet":
            if columns:
                df = pq.read_table(
                    io.BytesIO(obj["Body"].read()), columns
                ).to_pandas()
            else:
                df = pq.read_table(io.BytesIO(obj["Body"].read())).to_pandas()
        elif ftype == "csv":
            df = pd.read_csv(io.BytesIO(obj["Body"].read()), sep=sep)
        elif ftype == "json":
            data = json.loads(
                obj["Body"].read(), object_pairs_hook=collections.OrderedDict
            )
            df = None
        elif ftype == "pkl":

            my_mock_file = mock_file(mode="wb+")
            my_mock_file.write(obj["Body"].read())
            data = joblib.load(my_mock_file)

            df = None
        else:
            try:
                data = joblib.load(obj["Body"].read())
            except:
                raise ValueError("File Type not supported!")
        if df is not None:
            dfs.append(df)
    if len(dfs) == 1:
        data = dfs[0]
    elif len(dfs) > 1:
        data = pd.concat(dfs, axis=0)
    else:
        pass
    if data is None:
        raise ValueError("No Data was read in!")
    return data


def write_text_to_s3(bucket, key, content, encryption="aws:kms"):
    s3 = boto3.resource("s3")

    kwargs = {"Body": content}
    if encryption:
        kwargs["ServerSideEncryption"] = encryption

    try:
        s3.Object(bucket, key).load()
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            s3.Bucket(bucket).Object(key=key).put(**kwargs)
    else:
        s3.Bucket(bucket).Object(key=key).delete()
        s3.Bucket(bucket).Object(key=key).put(**kwargs)


def write_json_to_s3(bucket, key, data, encryption="aws:kms"):
    json_content = json.dumps(data, indent=2, sort_keys=True)
    write_text_to_s3(bucket, key, json_content, encryption)


def write_yaml_to_s3(bucket, key, data, encryption="aws:kms"):
    yaml_content = yaml.dump(data, indent=6, default_flow_style=False)
    write_text_to_s3(bucket, key, yaml_content, encryption)


def write_local_to_s3(data, path, mode="overwrite", encryption="aws:kms"):
    """Write local data to s3.

    Writes local data to s3. Will choose output types based on
    datatype of data to be written, either writing csv/txt or
    binary file.

    Parameters:
        data (?): data to write out to s3
        path (str): s3 path to write data to
        mode(str, opt): how to handle existing file, default is overwrite
        encryption(str, opt): what type of encryption to use server-side, if
                              any; set to None for no encryption
    Returns:
        Nothing!
    """
    if "pd" not in vars() and "pd" not in globals():
        import pandas as pd
    if "joblib" not in vars() and "joblib" not in globals():
        import joblib

    bucket, key = split_path_bucket_key(path)
    if isinstance(data, dict) | isinstance(data, pd.DataFrame):
        if isinstance(data, dict):
            # make dataframe
            file = pd.DataFrame.from_dict([data])
        else:
            file = data
        if mode.lower() == "append":
            try:
                ftype = re.findall(r"[\.]([a-zA-Z]{1,8})$", path)[0]
                oldfile = read_s3_to_local(path, ftype=ftype)
                combined = pd.concat([oldfile, file], ignore_index=True)
            except:
                combined = file
                print("couldn't read existing file")
            file = combined
        else:
            pass
        # write
        if sys.version_info > (3, 0):
            csv_buffer = io.StringIO()
        else:
            csv_buffer = io.BytesIO()
        file.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource("s3")
        if encryption is None:
            s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
        else:
            s3_resource.Object(bucket, key).put(
                Body=csv_buffer.getvalue(), ServerSideEncryption=encryption
            )
    else:

        my_mock_file = mock_file(mode="wb+")
        joblib.dump(data, my_mock_file)
        data_serialized = my_mock_file.read()

        s3 = boto3.resource("s3")
        if encryption is None:
            dump_s3 = (
                lambda f, data: s3.Bucket(bucket)
                .Object(key=f)
                .put(Body=data_serialized)
            )
        else:
            dump_s3 = (
                lambda f, data: s3.Bucket(bucket)
                .Object(key=f)
                .put(Body=data_serialized, ServerSideEncryption=encryption)
            )
        try:
            s3.Object(bucket, key).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                dump_s3(key, data_serialized)
        else:
            s3.Bucket(bucket).Object(key=key).delete()
            dump_s3(key, data_serialized)

    return


def copy_file_to_s3(local_path, s3_path):
    """
    Copy the local file from local_path to s3_path.

    Parameters:
        local_path (str): path for the local source file
        s3_path (str): path for the S3 destination file

    Returns: None
    """

    with open(local_path, "rb") as f:
        content = f.read()

    bucket, key = split_path_bucket_key(s3_path)
    write_text_to_s3(bucket, key, content)


def copy_dir_to_s3(local_path, s3_path):
    """
    Copy the local dir from local_path to s3_path.

    Parameters:
        local_path (str): path for the local source dir
        s3_path (str): path for the S3 destination dir

    Returns: None
    """

    if not os.path.isdir(local_path):
        raise Exception("Dir {} does not exist".format(local_path))

    for item in os.scandir(local_path):
        if item.is_file():
            s3_key = "{}/{}".format(s3_path, item.name)
            copy_file_to_s3(item.path, s3_key)
        elif item.is_dir():
            s3_subpath = "{}/{}".format(s3_path, item.name)
            copy_dir_to_s3(item.path, s3_subpath)
