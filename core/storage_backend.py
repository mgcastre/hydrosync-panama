"""
Title: storage_backend.py
Author: M. G. Castrellon
Date: 9 June 2026

Description:
Defines a protocol for storage backends and provides implementations 
for local filesystem, S3, and a fake in-memory backend for testing.
"""

# Load libraries
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class StorageBackend(Protocol):
    def save(self, content: bytes, relative_path: Path) -> str:
        ...

class LocalStorage:
    def __init__(self, root: Path):
        self.root = root
    
    def save(self, content: bytes, relative_path: Path) -> str:
        destination = self.root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        return destination.as_posix()

class S3Storage:
    def __init__(self, bucket_name: str, s3_client: Any):
        self.bucket_name = bucket_name
        self.client = s3_client
    
    def save(self, content: bytes, relative_path: Path) -> str:
        object_key = relative_path.as_posix()
        object_uri = f"s3://{self.bucket_name}/{object_key}"
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=object_key,
            Body=content
        )
        return object_uri

class FakeStorage:
    def __init__(self):
        self.outputs = dict()

    def save(self, content: bytes, relative_path: Path) -> str:
        fake_location = f"fake://{relative_path.as_posix()}"
        self.outputs[fake_location] = content
        return fake_location
