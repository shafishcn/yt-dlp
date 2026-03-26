import mimetypes
import os

from .common import PostProcessor
from ..dependencies import boto3
from ..utils import PostProcessingError


class S3UploadPP(PostProcessor):
    """Upload downloaded files to an S3-compatible object store."""

    def __init__(
            self, downloader=None, *,
            bucket=None, key=None, prefix='',
            endpoint_url=None, region_name=None,
            aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None,
            profile_name=None, storage_class=None, acl=None,
            content_type=None, delete_local=False, path_style=False):
        super().__init__(downloader)
        self.bucket = bucket
        self.key = key
        self.prefix = prefix
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.profile_name = profile_name
        self.storage_class = storage_class
        self.acl = acl
        self.content_type = content_type
        self.delete_local = self._parse_bool(delete_local, name='delete_local')
        self.path_style = self._parse_bool(path_style, name='path_style')

    @staticmethod
    def _parse_bool(value, *, name):
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ('1', 'true', 'yes', 'on'):
                return True
            if normalized in ('', '0', 'false', 'no', 'off'):
                return False
        raise ValueError(f'Invalid value for {name}: {value!r}')

    def _evaluate(self, value, info, *, default=''):
        if not value:
            return default
        return self._downloader.evaluate_outtmpl(value, info)

    @staticmethod
    def _join_key(prefix, name):
        if not prefix:
            return name
        return f'{prefix.rstrip("/")}/{name.lstrip("/")}'

    def _build_key(self, info):
        if self.key:
            return self._evaluate(self.key, info)
        prefix = self._evaluate(self.prefix, info)
        return self._join_key(prefix, os.path.basename(info['filepath']))

    def _build_extra_args(self, info):
        extra_args = {}
        content_type = self._evaluate(self.content_type, info)
        if not content_type:
            content_type = mimetypes.guess_type(info['filepath'])[0]
        if content_type:
            extra_args['ContentType'] = content_type

        storage_class = self._evaluate(self.storage_class, info)
        if storage_class:
            extra_args['StorageClass'] = storage_class

        acl = self._evaluate(self.acl, info)
        if acl:
            extra_args['ACL'] = acl
        return extra_args

    def _get_client(self, info):
        if boto3 is None:
            raise PostProcessingError(
                'boto3 is not installed. Install it with `python3 -m pip install "yt-dlp[s3]"`')

        session_kwargs = {}
        profile_name = self._evaluate(self.profile_name, info)
        if profile_name:
            session_kwargs['profile_name'] = profile_name
        session = boto3.session.Session(**session_kwargs)

        client_kwargs = {}
        for key in ('endpoint_url', 'region_name', 'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
            value = self._evaluate(getattr(self, key), info)
            if value:
                client_kwargs[key] = value

        if self.path_style:
            try:
                from botocore.config import Config
            except ImportError as err:
                raise PostProcessingError(f'botocore is unavailable: {err}') from err
            client_kwargs['config'] = Config(s3={'addressing_style': 'path'})

        return session.client('s3', **client_kwargs)

    def run(self, info):
        bucket = self._evaluate(self.bucket, info).strip() if self.bucket else ''
        if not bucket:
            raise PostProcessingError('S3UploadPP requires "bucket=<bucket-name>"')

        key = self._build_key(info)
        if not key:
            raise PostProcessingError('S3UploadPP requires a non-empty object key')

        extra_args = self._build_extra_args(info)
        client = self._get_client(info)
        self.to_screen(f'Uploading "{info["filepath"]}" to "s3://{bucket}/{key}"')
        try:
            client.upload_file(info['filepath'], bucket, key, ExtraArgs=extra_args or None)
        except Exception as err:
            raise PostProcessingError(f'S3 upload failed: {err}') from err

        info['s3_bucket'] = bucket
        info['s3_key'] = key
        info['s3_url'] = f's3://{bucket}/{key}'
        if self.delete_local:
            self.to_screen(f'Deleting local file "{info["filepath"]}" after successful upload')
            os.remove(info['filepath'])
        return [], info
