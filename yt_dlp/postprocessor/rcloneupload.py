import os
import subprocess

from .common import PostProcessor
from ..utils import (
    Popen,
    PostProcessingError,
    check_executable,
)


class RcloneUploadPP(PostProcessor):
    """Upload downloaded files to an object-storage backend via rclone.

    This keeps yt-dlp itself free of provider SDKs while still supporting
    any backend that rclone can address, including MinIO, GCS, OSS, and UpYun.
    """

    def __init__(self, downloader=None, remote=None, target=None, prefix='', rclone='rclone', delete_local=False):
        super().__init__(downloader)
        self.remote = remote
        self.target = target
        self.prefix = prefix
        self.rclone = rclone or 'rclone'
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

    def _evaluate_template(self, template, info, *, default=''):
        if not template:
            return default
        return self._downloader.evaluate_outtmpl(template, info)

    @staticmethod
    def _join_remote_path(base, path):
        if not path:
            return base
        path = path.replace(os.sep, '/').lstrip('/')
        if base.endswith(':'):
            return f'{base}{path}'
        return f'{base.rstrip("/")}/{path}'

    def _destination(self, info):
        remote = self._evaluate_template(self.remote, info).strip() if self.remote else ''
        if not remote:
            raise PostProcessingError('RcloneUploadPP requires "remote=<name:bucket_or_prefix>"')

        if self.target:
            object_path = self._evaluate_template(self.target, info)
        else:
            prefix = self._evaluate_template(self.prefix, info)
            object_path = self._join_remote_path(prefix.rstrip('/'), os.path.basename(info['filepath']))
        return self._join_remote_path(remote, object_path)

    def _get_command(self, info):
        destination = self._destination(info)
        rclone = check_executable(self.rclone, ['version'])
        if not rclone:
            raise PostProcessingError(
                f'Rclone executable "{self.rclone}" not found. Install rclone or pass rclone=<path>')

        cmd = [
            rclone,
            'copyto',
            info['filepath'],
            destination,
            *self._configuration_args('rclone'),
        ]
        return cmd, destination

    def run(self, info):
        cmd, destination = self._get_command(info)
        self.to_screen(f'Uploading "{info["filepath"]}" to "{destination}" via rclone')
        stdout, stderr, return_code = Popen.run(
            cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if return_code != 0:
            raise PostProcessingError((stderr or stdout or '').strip() or f'Rclone failed with exit code {return_code}')

        info['rclone_destination'] = destination
        if self.delete_local:
            self.to_screen(f'Deleting local file "{info["filepath"]}" after successful upload')
            os.remove(info['filepath'])
        return [], info
