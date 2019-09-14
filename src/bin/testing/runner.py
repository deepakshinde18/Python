import unittest
import os


def suite():
    s = unittest.TestSuite()
    loader = unittest.TestLoader()
    tmp = loader.discover(os.path.dirname(__file__))
    s.addTests(tmp)
    return s


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
