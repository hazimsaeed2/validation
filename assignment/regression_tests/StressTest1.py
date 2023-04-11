import copy
import subprocess

import pe_memberdna.testing_support.regression_framework.regression as regression


class StressTest1(regression.RegressionTest):
    """
    A minimal example of a regression test
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        """
        Since this is just a stress test, we aren't focused on correctness.  If there is an assignment file that
        assigned to everyone then pass
        pass
        :return:
        """
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        # Logic that shows assignment finished
        final_mailhouse = self.spark.read.option("header", "true").csv(
            self.config.paths["MAILFILE"]
        )
        print(final_mailhouse.count())
        assert (
            final_mailhouse.select("mbrshp_sid").distinct().count() == 4265803
        )

    def execute_assignment_scripts(
        self,
        _scripts=[
            "create_coupons.py",
            "assign_offers.py",
            "subsample_population.py",
            "backtest_sizing.py",
            "generate_output_file.py",
            "qc_assignments.py",
        ],
    ):
        scripts = copy.deepcopy(_scripts)
        self.log.info("Running the following scripts: {}".format(scripts))
        last_status = 0
        for script in scripts:
            if last_status != 0:
                continue
            call = [
                "spark-submit",
                "--conf",
                "spark.default.parallelism=2048",
                "--conf",
                "spark.sql.shuffle.partitions=2048",
                "--conf",
                "spark.python.worker.memory=2048",
                "--conf",
                "spark.sql.broadcastTimeout=600",
                "--conf",
                "spark.executor.cores=2",
                "--num-executors",
                "100000",
                "./scripts/{}".format(script),
                "--conf",
                "./conf/test/{}_config.yml".format(self.test_name),
            ]
            last_status = subprocess.call(call)
            outcome = [script, last_status]
            self.outcomes.append(outcome)
            self.log.info(outcome)


if __name__ == "__main__":
    StressTest1.execute_test()
