import json
import mimetypes
import os

from .common import PostProcessor
from ..dependencies import google_cloud_storage, google_service_account
from ..utils import PostProcessingError


class GCSUploadPP(PostProcessor):
    """Upload downloaded files to Google Cloud Storage."""

    def __init__(
            self, downloader=None, *,
            bucket=None, key=None, prefix='',
            project=None, credentials_file=None, credentials_json=None,
            content_type=None, predefined_acl=None, delete_local=False):
        super().__init__(downloader)
        self.bucket = bucket
        self.key = key
        self.prefix = prefix
        self.project = project
        self.credentials_file = credentials_file
        self.credentials_json = credentials_json
        self.content_type = content_type
        self.predefined_acl = predefined_acl
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
        if google_cloud_storage is None:
            raise PostProcessingError(
                'google-cloud-storage is not installed. Install it with `python3 -m pip install "yt-dlp[gcs]"`')

        project = self._evaluate(self.project, info) or None
        credentials_file = self._evaluate(self.credentials_file, info) or None
        credentials_json = self._evaluate(self.credentials_json, info) or None

        credentials = None
        if credentials_file or credentials_json:
            if google_service_account is None:
                raise PostProcessingError('google-auth service account support is unavailable')
            if credentials_json:
                credentials = google_service_account.Credentials.from_service_account_info(json.loads(credentials_json))
            else:
                credentials = google_service_account.Credentials.from_service_account_file(credentials_file)

        return google_cloud_storage.Client(project=project, credentials=credentials)

    def run(self, info):
        bucket_name = self._evaluate(self.bucket, info).strip() if self.bucket else ''
        if not bucket_name:
            raise PostProcessingError('GCSUploadPP requires "bucket=<bucket-name>"')

        key = self._build_key(info)
        if not key:
            raise PostProcessingError('GCSUploadPP requires a non-empty object key')

        client = self._get_client(info)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(key)

        content_type = self._evaluate(self.content_type, info) or mimetypes.guess_type(info['filepath'])[0]
        predefined_acl = self._evaluate(self.predefined_acl, info) or None

        self.to_screen(f'Uploading "{info["filepath"]}" to "gs://{bucket_name}/{key}"')
        try:
            blob.upload_from_filename(
                info['filepath'],
                content_type=content_type,
                predefined_acl=predefined_acl)
        except Exception as err:
            raise PostProcessingError(f'GCS upload failed: {err}') from err

        info['gcs_bucket'] = bucket_name
        info['gcs_key'] = key
        info['gcs_url'] = f'gs://{bucket_name}/{key}'
        if self.delete_local:
            self.to_screen(f'Deleting local file "{info["filepath"]}" after successful upload')
            os.remove(info['filepath'])
        return [], info
