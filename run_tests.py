import unittest
from Databular.Tests import test_databular

loader = unittest.TestLoader()
suite = unittest.TestSuite()

# add tests to the test suite

suite.addTest(loader.loadTestsFromModule(test_databular))


# initialize a test runner and run the test suite

runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)