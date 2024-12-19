import logging
import os
from pathlib import Path
from typing import List

import boto3

from z3 import asyncable

logger = logging.getLogger(__name__)

session = boto3.Session()
client = session.client("s3")


def list_object_keys(
    bucket_name: str, prefix: str, exclude_folders: bool = True
) -> List[str]:
    if prefix.startswith("/"):
        raise ValueError("prefix must not start with /")

    object_keys = []

    try:
        # paginator 없이 listing 할 경우 최대 1,000 개만 listing 됨.
        paginator = client.get_paginator("list_objects_v2")
        for result in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            contents = result.get("Contents", [])
            _object_keys = map(lambda x: x["Key"], contents)
            object_keys.extend(_object_keys)
    except Exception as e:
        logger.exception(f"Failed to list objects:\n{e}")
        raise e
    finally:
        client.close()

    if exclude_folders:
        object_keys = list(filter(lambda x: not x.endswith("/"), object_keys))

    return object_keys


def copy_object(
    src_bucket_name: str,
    src_object_key: str,
    dest_bucket_name: str,
    dest_object_key: str,
):
    if src_object_key.startswith("/") or dest_object_key.startswith("/"):
        raise ValueError("object_key must not start with /")

    try:
        _ = client.copy_object(
            Bucket=dest_bucket_name,
            Key=dest_object_key,
            CopySource={
                "Bucket": src_bucket_name,
                "Key": src_object_key,
            },
        )
    except Exception as e:
        logger.exception(f"Failed to copy object:\n{e}")
        raise e
    finally:
        client.close()


def delete_object(bucket_name: str, object_key: str):
    if object_key.startswith("/"):
        raise ValueError("object_key must not start with /")

    try:
        _ = client.delete_object(Bucket=bucket_name, Key=object_key)
    except Exception as e:
        logger.exception(f"Failed to delete object '{bucket_name}/{object_key}':\n{e}")
        raise e
    finally:
        client.close()


def move_object(
    src_bucket_name: str,
    src_object_key: str,
    dest_bucket_name: str,
    dest_object_key: str,
):
    if src_object_key.startswith("/") or dest_object_key.startswith("/"):
        raise ValueError("object_key must not start with /")

    copy_object(
        src_bucket_name,
        src_object_key,
        dest_bucket_name,
        dest_object_key,
    )
    delete_object(src_bucket_name, src_object_key)


def download_object(bucket_name: str, object_key: str, local_path: Path):
    if object_key.startswith("/"):
        raise ValueError("object_key must not start with /")

    try:
        logger.info(
            f"Downloading object '{bucket_name}/{object_key}' to '{local_path}'"
        )
        response = client.get_object(Bucket=bucket_name, Key=object_key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in response["Body"].iter_chunks():
                f.write(chunk)
    except Exception as e:
        logger.exception(
            f"Failed to download a object '{bucket_name}/{object_key}':\n{e}"
        )
        raise e
    finally:
        client.close()


def download_folder(bucket_name: str, prefix: str, local_path: Path):
    if prefix.startswith("/"):
        raise ValueError("prefix must not start with /")

    object_keys = list_object_keys(bucket_name, prefix)

    for object_key in object_keys:
        # 파일일 경우에만 다운로드
        if object_key.endswith("/"):
            continue

        relative_object_key = Path(object_key).relative_to(prefix)
        dest_local_path = local_path / relative_object_key

        download_object(bucket_name, object_key, dest_local_path)


def put_object(bucket_name: str, object_key: str, local_path: Path):
    if object_key.startswith("/"):
        raise ValueError("object_key must not start with /")

    try:
        with open(local_path, "rb") as f:
            file_data = f.read()
            client.put_object(Bucket=bucket_name, Key=object_key, Body=file_data)
    except Exception as e:
        logger.exception(f"Failed to upload object '{bucket_name}/{object_key}':\n{e}")
        raise e
    finally:
        client.close()


def put_folder(bucket_name: str, prefix: str, local_path: Path):
    if prefix.startswith("/"):
        raise ValueError("prefix must not start with /")

    for local_file_path in local_path.rglob("*"):
        if not local_file_path.is_file():
            continue

        relative_path = local_file_path.relative_to(local_path)
        s3_key = str(Path(prefix) / relative_path)

        put_object(bucket_name, s3_key, local_file_path)


async def alist_object_keys(
    bucket_name: str, prefix: str, exclude_folders: bool = True
) -> List[str]:
    return await asyncable.run_in_executor(
        None,
        list_object_keys,
        bucket_name=bucket_name,
        prefix=prefix,
        exclude_folders=exclude_folders,
    )


async def acopy_object(
    src_bucket_name: str,
    src_object_key: str,
    dest_bucket_name: str,
    dest_object_key: str,
):
    await asyncable.run_in_executor(
        None,
        copy_object,
        src_bucket_name=src_bucket_name,
        src_object_key=src_object_key,
        dest_bucket_name=dest_bucket_name,
        dest_object_key=dest_object_key,
    )


async def adelete_object(bucket_name: str, object_key: str):
    return await asyncable.run_in_executor(
        None,
        delete_object,
        bucket_name=bucket_name,
        object_key=object_key,
    )


async def amove_object(
    src_bucket_name: str,
    src_object_key: str,
    dest_bucket_name: str,
    dest_object_key: str,
):
    return await asyncable.run_in_executor(
        None,
        move_object,
        src_bucket_name=src_bucket_name,
        src_object_key=src_object_key,
        dest_bucket_name=dest_bucket_name,
        dest_object_key=dest_object_key,
    )


async def adownload_object(bucket_name: str, object_key: str, local_path: Path):
    return await asyncable.run_in_executor(
        None,
        download_object,
        bucket_name=bucket_name,
        object_key=object_key,
        local_path=local_path,
    )


async def adownload_folder(bucket_name: str, prefix: str, local_path: Path):
    return await asyncable.run_in_executor(
        None,
        download_folder,
        bucket_name=bucket_name,
        prefix=prefix,
        local_path=local_path,
    )


async def aput_object(bucket_name: str, object_key: str, local_path: Path):
    return await asyncable.run_in_executor(
        None,
        put_object,
        bucket_name=bucket_name,
        object_key=object_key,
        local_path=local_path,
    )


async def aput_folder(bucket_name: str, prefix: str, local_path: Path):
    return await asyncable.run_in_executor(
        None,
        put_folder,
        bucket_name=bucket_name,
        prefix=prefix,
        local_path=local_path,
    )
