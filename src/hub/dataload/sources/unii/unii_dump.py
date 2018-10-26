import os, sys, re, time
import bs4
import dateutil.parser as dtparser
import datetime

from config import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import HTTPDumper, DumperException
from biothings.utils.common import unzipall


class UniiDumper(HTTPDumper):

    SRC_NAME = "unii"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)

    SCHEDULE = "0 12 * * *"
    HOMEPAGE_URL = "https://fdasis.nlm.nih.gov/srs"
    DATA_URL = "https://fdasis.nlm.nih.gov/srs/download/srs/UNII_Data.zip"

    def get_latest_release(self):
        res = self.client.get(self.__class__.HOMEPAGE_URL)
        html = bs4.BeautifulSoup(res.text,"lxml")
        # link containing the latest date version
        version = html.find(attrs={"href":"/srs/jsp/srs/uniiListDownload.jsp"}).text
        m = re.match("UNII List download \(updated (.*)\)",version)
        try:
            latest = datetime.date.strftime(dtparser.parse(m.groups()[0]),"%Y-%m-%d")
            return latest
        except Exception as e:
            raise DumperException("Can't find or parse date from URL '%s': %s" % (self.__class__.HOMEPAGE_URL,e))

    def create_todump_list(self,force=False,**kwargs):
        self.release = self.get_latest_release()
        if force or not self.src_doc or (self.src_doc and self.src_doc.get("download",{}).get("release") < self.release):
            # if new release, that link points to that latest release
            local = os.path.join(self.new_data_folder,os.path.basename(self.DATA_URL))
            self.to_dump.append({"remote":self.DATA_URL, "local":local})

    def post_dump(self, *args, **kwargs):
        unzipall(self.new_data_folder)

