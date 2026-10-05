"""Accès bas niveau au data lake (MinIO, API S3) pour la couche bronze."""
from __future__ import annotations

import json
import logging
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from meteo.config import Settings

log = logging.getLogger(__name__)


class Lake:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.bucket = cfg.lake_bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url=cfg.s3_endpoint,
            aws_access_key_id=cfg.s3_access_key,
            aws_secret_access_key=cfg.s3_secret_key,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def ensure_buckets(self, retries: int = 30) -> None:
        for attempt in range(retries):
            try:
                existing = {b["Name"] for b in self.s3.list_buckets().get("Buckets", [])}
                break
            except (EndpointConnectionError, ClientError) as exc:
                log.info("MinIO pas encore prêt (%s), nouvel essai...", exc)
                time.sleep(2)
        else:
            raise RuntimeError("MinIO injoignable")
        for name in (self.cfg.lake_bucket, self.cfg.mlflow_bucket):
            if name not in existing:
                self.s3.create_bucket(Bucket=name)
                log.info("Bucket créé : %s", name)

    def put_json(self, key: str, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")

    def get_json(self, key: str):
        return json.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        return keys
