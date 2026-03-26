import mimetypes
import os

from .common import PostProcessor
from ..dependencies import upyun
from ..utils import PostProcessingError


class UpYunUploadPP(PostProcessor):
    """Upload downloaded files to UpYun using the Python SDK."""

    def __init__(
            self, downloader=None, *,
            service=None, bucket=None, key=None, prefix='',
            operator=None, password=None, endpoint=None, timeout=None,
            content_type=None, checksum=True, delete_local=False):
        super().__init__(downloader)
        self.service = service or bucket
        self.key = key
        self.prefix = prefix
        self.operator = operator
        self.password = password
        self.endpoint = endpoint
        self.timeout = timeout
        self.content_type = content_type
        self.checksum = self._parse_bool(checksum, name='checksum')
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
            return self._evaluate(self.key, info).lstrip('/')
        prefix = self._evaluate(self.prefix, info)
        return self._join_key(prefix, os.path.basename(info['filepath']))

    def _get_client(self, info, service):
        if upyun is None:
            raise PostProcessingError(
                'upyun is not installed. Install it with `python3 -m pip install "yt-dlp[upyun]"`')

        operator = self._evaluate(self.operator, info).strip() if self.operator else ''
        password = self._evaluate(self.password, info).strip() if self.password else ''
        if not operator:
            raise PostProcessingError('UpYunUploadPP requires "operator=<operator-name>"')
        if not password:
            raise PostProcessingError('UpYunUploadPP requires "password=<operator-password>"')

        client_kwargs = {}
        endpoint = self._evaluate(self.endpoint, info) or None
        if endpoint:
            client_kwargs['endpoint'] = endpoint

        timeout = self._evaluate(self.timeout, info) or None
        if timeout:
            try:
                client_kwargs['timeout'] = float(timeout)
            except ValueError as err:
                raise PostProcessingError(f'Invalid UpYun timeout: {timeout!r}') from err

        return upyun.UpYun(service, operator, password, **client_kwargs)

    def run(self, info):
        service = self._evaluate(self.service, info).strip() if self.service else ''
        if not service:
            raise PostProcessingError('UpYunUploadPP requires "service=<service-name>" or "bucket=<service-name>"')

        key = self._build_key(info)
        if not key:
            raise PostProcessingError('UpYunUploadPP requires a non-empty object key')

        client = self._get_client(info, service)
        headers = {}
        content_type = self._evaluate(self.content_type, info) or mimetypes.guess_type(info['filepath'])[0]
        if content_type:
            headers['Content-Type'] = content_type

        self.to_screen(f'Uploading "{info["filepath"]}" to "upyun://{service}/{key}"')
        try:
            with open(info['filepath'], 'rb') as file_obj:
                client.put(key, file_obj, checksum=self.checksum, headers=headers or None)
        except Exception as err:
            raise PostProcessingError(f'UpYun upload failed: {err}') from err

        info['upyun_service'] = service
        info['upyun_key'] = key
        info['upyun_url'] = f'upyun://{service}/{key}'
        if self.delete_local:
            self.to_screen(f'Deleting local file "{info["filepath"]}" after successful upload')
            os.remove(info['filepath'])
        return [], info
