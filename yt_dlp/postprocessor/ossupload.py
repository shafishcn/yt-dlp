import os

from .common import PostProcessor
from ..dependencies import alibabacloud_oss_v2
from ..utils import PostProcessingError


class OSSUploadPP(PostProcessor):
    """Upload downloaded files to Alibaba Cloud OSS using the official Python SDK."""

    def __init__(
            self, downloader=None, *,
            bucket=None, key=None, prefix='',
            region=None, endpoint=None,
            access_key_id=None, access_key_secret=None, security_token=None,
            delete_local=False):
        super().__init__(downloader)
        self.bucket = bucket
        self.key = key
        self.prefix = prefix
        self.region = region
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.security_token = security_token
        self.delete_local = self._parse_bool(delete_local, name='delete_local')

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

    def _get_client(self, info):
        if alibabacloud_oss_v2 is None:
            raise PostProcessingError(
                'alibabacloud-oss-v2 is not installed. Install it with `python3 -m pip install "yt-dlp[oss]"`')

        region = self._evaluate(self.region, info)
        if not region:
            raise PostProcessingError('OSSUploadPP requires "region=<region-id>"')

        oss = alibabacloud_oss_v2
        config = oss.config.load_default()
        config.region = region

        endpoint = self._evaluate(self.endpoint, info) or None
        if endpoint:
            config.endpoint = endpoint

        access_key_id = self._evaluate(self.access_key_id, info) or None
        access_key_secret = self._evaluate(self.access_key_secret, info) or None
        security_token = self._evaluate(self.security_token, info) or None
        if access_key_id and access_key_secret:
            config.credentials_provider = oss.credentials.StaticCredentialsProvider(
                access_key_id=access_key_id,
                access_key_secret=access_key_secret,
                security_token=security_token,
            )
        else:
            config.credentials_provider = oss.credentials.EnvironmentVariableCredentialsProvider()

        return oss.Client(config)

    def run(self, info):
        bucket_name = self._evaluate(self.bucket, info).strip() if self.bucket else ''
        if not bucket_name:
            raise PostProcessingError('OSSUploadPP requires "bucket=<bucket-name>"')

        key = self._build_key(info)
        if not key:
            raise PostProcessingError('OSSUploadPP requires a non-empty object key')

        client = self._get_client(info)
        oss = alibabacloud_oss_v2
        self.to_screen(f'Uploading "{info["filepath"]}" to "oss://{bucket_name}/{key}"')
        try:
            with open(info['filepath'], 'rb') as file_obj:
                client.put_object(oss.PutObjectRequest(
                    bucket=bucket_name,
                    key=key,
                    body=file_obj,
                ))
        except Exception as err:
            raise PostProcessingError(f'OSS upload failed: {err}') from err

        info['oss_bucket'] = bucket_name
        info['oss_key'] = key
        info['oss_url'] = f'oss://{bucket_name}/{key}'
        if self.delete_local:
            self.to_screen(f'Deleting local file "{info["filepath"]}" after successful upload')
            os.remove(info['filepath'])
        return [], info
