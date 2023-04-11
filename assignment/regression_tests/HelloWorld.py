import pe_memberdna.testing_support.regression_framework.regression as regression


class HelloWorld(regression.RegressionTest):
    """
    A minimal example of a regression test
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        self.log.info("Smoke test...")
        self.spark.read.option("header", "true").csv(
            self.config.paths["MAILFILE"]
        ).count()


if __name__ == "__main__":
    HelloWorld.execute_test()
