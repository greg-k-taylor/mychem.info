import sys
import os

src_path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application
from tests.tests import MyChemTest
from biothings.tests.test_helper import TornadoRequestHelper
from web.settings import MyDrugWebSettings
#import unittest

class MyChemTornadoTest(AsyncHTTPTestCase, MyChemTest):
    def __init__(self, methodName='runTest', **kwargs):
        super(AsyncHTTPTestCase, self).__init__(methodName, **kwargs)
        self.h = TornadoRequestHelper(self)
        self._settings = MyDrugWebSettings(config='config')

    def get_app(self):
        return Application(self._settings.generate_app_list())

#if __name__ == "__main__":
#    unittest.TextTestRunner().run(MyChemTornadoTest.suite())
