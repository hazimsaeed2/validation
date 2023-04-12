import pe_memberdna.testing_support.regression_framework.regression as regression


class FailTest(regression.RegressionTest):
    """
    A minimal example of a regression test
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    @unittest.expectedFailure
    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data()

        self.log.error("This test will fail.")
        self.assertEqual(False, True)

    def execute_assignment_scripts(self):
        super(FailTest, self).execute_assignment_scripts(_scripts=[])


if __name__ == "__main__":
    FailTest.execute_test()
