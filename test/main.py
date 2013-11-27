#!/usr/bin/env python

import unittest
import test_McApp
import test_store




if __name__ == '__main__':

    MCTestSuite = unittest.defaultTestLoader.discover(start_dir='.')

    unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(MCTestSuite))
