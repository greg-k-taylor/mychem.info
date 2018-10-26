import os
import os.path
import sys
import time

from config import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import FTPDumper, DumperException
from biothings.utils.common import gunzipall


class ChebiDumper(FTPDumper):

    SRC_NAME = "chebi"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    FTP_HOST = 'ftp.ebi.ac.uk'
    CWD_DIR = '/pub/databases/chebi/archive' # contains all releases

    SCHEDULE = "0 12 * * *"

    def get_release(self):
        self.client.cwd(self.__class__.CWD_DIR)
        releases = sorted(self.client.nlst())
        if len(releases) == 0:
            raise DumperException("Can't any release information in '%s'" % self.__class__.VERSION_DIR)
        self.release = releases[-1]

    def new_release_available(self):
        current_release = self.src_doc.get("download",{}).get("release")
        if not current_release or self.release > current_release:
            self.logger.info("New release '%s' found" % self.release)
            return True
        else:
            self.logger.debug("No new release found")
            return False

    def create_todump_list(self, force=False):
        self.get_release()
        if force or self.new_release_available():
            # get list of files to download
            remote_path = os.path.join(self.__class__.CWD_DIR,self.release,"SDF")
            self.client.cwd(remote_path)
            data_file = "ChEBI_complete.sdf.gz"
            remote = os.path.join(remote_path,data_file)
            local = os.path.join(self.new_data_folder,data_file)
            self.to_dump.append({"remote": remote,"local":local})

    def post_dump(self, *args, **kwargs):
        gunzipall(self.new_data_folder)


