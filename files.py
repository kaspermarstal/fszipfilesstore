"""
Files Pipeline

See documentation in topics/media-pipeline.rst
"""

from hashlib import md5
import os

from scrapy.pipelines.files import FilesPipeline, FSFilesStore, S3FilesStore
import zipfile
from datetime import datetime
from collections import defaultdict

class FSZipFilesStore(object):

    def __init__(self, basedir):
        if '://' in basedir:
            basedir = basedir.split('://', 1)[1]
        self.basedir = basedir
        self._mkdir(self.basedir)
        self.created_directories = defaultdict(set)
        self.zip_filesystem_path = os.path.join(self.basedir, 'archive.zip')

    def persist_file(self, path, buf, info, meta=None, headers=None):
        with zipfile.ZipFile(self.zip_filesystem_path, mode='a', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(path, buf.getvalue())

    def stat_file(self, path, info):
        try:
            with zipfile.ZipFile(self.zip_filesystem_path) as zf:
                zipinfo = zf.getinfo(path)
                last_modified = datetime(*zipinfo.date_time)
                checksum = md5(zf.extract(zipinfo))
        except:  # FIXME: catching everything!
            return {}

        return {'last_modified': last_modified, 'checksum': checksum}

    def _mkdir(self, dirname, domain=None):
        seen = self.created_directories[domain] if domain else set()
        if dirname not in seen:
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            seen.add(dirname)

class ZipFilesPipeline(FilesPipeline):

    STORE_SCHEMES = {
        '': FSZipFilesStore,
        'file': FSFilesStore,
        'zip': FSZipFilesStore,
        's3': S3FilesStore,
    }
