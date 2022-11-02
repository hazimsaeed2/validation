import datetime
import mock
import os
import unittest
import xmlrunner
import yaml

import memberdna.category_square.category_square.square as square
import memberdna.lib.misc as misc

from memberdna.testing_support.lib.utility import get_unique_id


class TestAutoDate(object):
    """
    This is a parent class for the unittests that test
    the automatic start date and end date in member
    and category DNA classes.
    """

    def del_cfg(self, key_in, config_in=None):
        """
        Intended for deleting start date
        and end date items from the config
        """
        raise NotImplementedError()

    def get_cfg(self, key_in, config_in=None):
        """
        Intended as getter method for start date
        and end date items from the config
        """
        raise NotImplementedError()

    def set_cfg(self, key_in, val_in, config_in=None):
        """
        Intended as setter method for start date
        and end date items from the config
        """
        raise NotImplementedError()

    def run_dna(self, config_in=None):
        """
        Intended as a method that executes the
        member DNA
        """
        raise NotImplementedError()

    def check_one_weekday(self, test_datetime_obj):
        """
        Initiates the Cube class while simulating an
        arbitrary date. Then performs a set of sanity
        checks.

        Args:
            test_datetime_obj - datetime object specifying
            the date to simulate

        Returns:

        """
        self.del_cfg("start")
        self.del_cfg("end")

        with mock.patch("memberdna.lib.misc.today_helper") as mock_misc:
            mock_misc.return_value = test_datetime_obj
            dna_obj = self.run_dna()

        start_datetime_obj = datetime.datetime.strptime(
            self.get_cfg("start", dna_obj), "%Y-%m-%d"
        )
        end_datetime_obj = datetime.datetime.strptime(
            self.get_cfg("end", dna_obj), "%Y-%m-%d"
        )

        self.assertEqual(
            misc.get_weekday_abbreviation(self.get_cfg("end", dna_obj)), "Sat"
        )

        if test_datetime_obj.weekday() == 5:
            self.assertEqual((test_datetime_obj - end_datetime_obj).days, 7)
        else:
            self.assertLessEqual(
                (test_datetime_obj - end_datetime_obj).days, 7
            )

        self.assertEqual((end_datetime_obj - start_datetime_obj).days, 31 * 14)

    def test_thursday(self):
        """
        Checks that the start_date and end_date
        are calculated correctly if initiated
        on thursday
        """
        self.check_one_weekday(
            datetime.datetime(2019, 7, 4, 12, 22, 44, 123456)
        )

    def test_saturday(self):
        """
        same as test_thursday only done for saturday.
        In this case the correct end_date should
        be current_date - 7 days
        """
        self.check_one_weekday(
            datetime.datetime(2019, 7, 6, 12, 22, 44, 123456)
        )

    def test_sunday(self):
        """
        same as test_thursday only done for sunday.
        In this case the correct end_date should
        be current_date - 1 day
        """
        self.check_one_weekday(
            datetime.datetime(2019, 7, 7, 12, 22, 44, 123456)
        )

    def test_date_set(self):
        """
        Checks that the start date and end remain
        unchanged when selected manually in the
        config file
        """
        end_date_datetime_obj = datetime.datetime(
            2019, 6, 22, 12, 22, 44, 123456
        )
        start_date_datetime_obj = datetime.datetime(
            2018, 3, 22, 12, 22, 44, 123456
        )

        self.set_cfg("start", "{:%Y-%m-%d}".format(start_date_datetime_obj))
        self.set_cfg("end", "{:%Y-%m-%d}".format(end_date_datetime_obj))

        dna_obj = self.run_dna()

        start_datetime_obj = datetime.datetime.strptime(
            self.get_cfg("start", dna_obj), "%Y-%m-%d"
        )

        end_datetime_obj = datetime.datetime.strptime(
            self.get_cfg("end", dna_obj), "%Y-%m-%d"
        )

        self.assertEqual(self.get_cfg("start"), self.get_cfg("start", dna_obj))

        self.assertEqual(self.get_cfg("end"), self.get_cfg("end", dna_obj))

    def test_start_date_or_end_date_missing(self):
        """
        Checks that the start date and end remain
        unchanged when selected manually in the
        config file
        """
        end_date_datetime_obj = datetime.datetime(
            2019, 6, 22, 12, 22, 44, 123456
        )
        start_date_datetime_obj = datetime.datetime(
            2018, 3, 22, 12, 22, 44, 123456
        )

        self.set_cfg("start", "{:%Y-%m-%d}".format(start_date_datetime_obj))
        self.del_cfg("end")

        with self.assertRaises(Exception) as context:
            dna_obj = self.run_dna()

        self.del_cfg("start")
        self.set_cfg("end", "{:%Y-%m-%d}".format(end_date_datetime_obj))

        with self.assertRaises(Exception) as context:
            dna_obj = self.run_dna()


class TestCategoryDnaAutoDate(TestAutoDate, unittest.TestCase):
    """
    This unittest verifies that the start and the end dates are calculated
    correctly after the introduction of the feature that calculates them
    automaticaly
    """

    def setUp(self):
        """
        Loads the simulated config files for category_square
        """
        test_name = self.__class__.__name__
        unique_id = get_unique_id(test_name)

        simulated_config_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "data/simulated_config.yaml.{}".format(unique_id),
        )

        simulated_reqs_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "data/simulated_reqs.yaml.{}".format(unique_id),
        )

        templates = [
            simulated_config_path,
            simulated_reqs_path,
        ]
        for template in templates:
            original = template.replace(".{}".format(unique_id), "")
            with open(original, "r") as config_file:
                config = config_file.read()
            with open(template, "w") as config_file:
                config = config.replace("UNIQUE_ID", unique_id)
                config_file.write(config)

        with open(simulated_config_path) as config_file:
            self.config = yaml.load(config_file, Loader=yaml.FullLoader)

        with open(simulated_reqs_path) as reqs_file:
            self.reqs = yaml.load(reqs_file, Loader=yaml.FullLoader)

    def del_cfg(self, key_in, config_in=None):
        """
        Deletes the start or end from
        the config
        """
        del (config_in.config if config_in else self.config)[key_in]

    def get_cfg(self, key_in, config_in=None):
        """
        Returns the value of start or end from
        the config
        """
        return (config_in.config if config_in else self.config)[key_in]

    def set_cfg(self, key_in, val_in, config_in=None):
        """
        Sets the value of start or end in
        the config
        """
        (config_in.config if config_in else self.config)[key_in] = val_in

    def run_dna(self, config_in=None):
        """
        Executes the category DNA
        """
        return square.Square(
            self.reqs["prod"], (config_in if config_in else self.config)
        )


if __name__ == "__main__":

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
