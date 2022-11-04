"""
Contains all static methods for building the source dataframes.
"""
import datetime as dt

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


class Loader:

    spark = SparkSession.builder.appName("CatSquareBuilder").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite","CORRECTED")

    @staticmethod
    def __high_spend_filter(detail, spend_filter):
        """
        We need to filter out members who have spent more than 500k in a given year.
        We do this by accumulating their spend by year, taking the max, and if that max
        is over 500k, we exclude that member.
        """

        selected = (
            detail.select("MBRSHP_SID", "EXTENDED_PRC_AMT", "PURCH_DT")
            .withColumn("YEAR", F.year("PURCH_DT"))
            .drop("PURCH_DT")
        )

        yearly_spend = selected.groupBy("MBRSHP_SID", "YEAR").agg(
            F.sum("EXTENDED_PRC_AMT").alias("SPEND")
        )

        max_yearly = yearly_spend.groupBy("MBRSHP_SID").agg(
            F.max("SPEND").alias("MAX_SPEND")
        )

        included_members = max_yearly.filter(
            max_yearly["MAX_SPEND"] < spend_filter
        ).drop("MAX_SPEND")

        return included_members

    @staticmethod
    def __filter_detail(detail, key, mbrs, config):
        """
        Filters detail for analysis.
        Only trips (defined by client) and member numbers that do not begin with 088.
        Member numbers that begin with 088 are dummy members.

        Args:
            detail: The detail dataframe
            mbrs: The MBRSHP_SID to MBRSHP_NBR mapping
        """
        spend_filter = config["high_spend"]

        # these are merchant category codes that refer to stores within a store (cafe, etc),
        # we don't consider these as true 'in-store' purchases, and so they are excluded here
        mc_cds = ["402030190", "402030191", "203010098"]
        excl_club = "088%"

        mbrs = mbrs.select("MBRSHP_SID", "MBRSHP_NBR").dropDuplicates()

        detail = detail.join(mbrs, ["MBRSHP_SID"], "left_outer")

        filtered_detail = (
            detail.filter(~detail.MC_CD.isin(mc_cds))
            .filter(~detail.MBRSHP_NBR.like(excl_club))
            .filter(detail.MBRSHP_SID.isNotNull())
        )

        # only include members who spent less than 500k per year
        member_spend = Loader.__high_spend_filter(
            filtered_detail, spend_filter
        )

        filtered_detail = filtered_detail.join(
            member_spend, "MBRSHP_SID", "inner"
        )

        return filtered_detail

    @staticmethod
    def __load_intermediate(filepath, start, end, spark):
        """
        Loads a transaction intermediate and filters for given time range.

        Args:
            filepath: path to the intermediate
            start: the start date to filter FISCAL_WEEK_END by
            end: the end date to filter FISCAL_WEEK_END by
            spark: the spark session

        Returns:
            Persisted dataframe with the transaction intermediate data.
        """
        print(f"\n Going to read the file {filepath}")
        df = spark.read.parquet(filepath)
        df = df.filter(df.FISCAL_WEEK_END.between(start, end)).persist()
        return df

    @staticmethod
    def __load_master(filepath, spark):
        """
        Loads a master file, persists the dataframe.

        Args:
            filepath: path to the intermediate
            spark: the spark session

        Returns:
            Persisted dataframe containing the master data
        """
        df = spark.read.parquet(filepath).persist()
        return df

    @staticmethod
    def __load_template(filepath, spark):
        """
        Loads a csv that's updated often
        """

        df = spark.read.csv(filepath, header=True, escape='"')
        return df

    @staticmethod
    def __seasonality_start(end, seasonality):
        delta = dt.timedelta(days=365 * seasonality)

        date_format = "%Y-%m-%d"
        last_week_dt = dt.datetime.strptime(end, date_format)

        start_week = last_week_dt - delta

        start_week_str = start_week.strftime(date_format)

        return start_week_str

    @staticmethod
    def __add_brand_to_detail(item_with_brand, detail):
        """
        Adds BRAND_CD and BRAND_DESC to detail table.
        May need to be removed is detail creation is modified.

        Args:
            item_with_brand: item table with brand info
            detail: detail table

        Returns:
            detail_with_brand: detail table with brand info
        """
        art_and_brand = item_with_brand.select(
            "ARTICLE_NBR", "BRAND_CD", "BRAND_DESC"
        )
        art_and_brand = art_and_brand.dropDuplicates(["ARTICLE_NBR"])
        detail = detail.join(art_and_brand, "ARTICLE_NBR", "left")
        return detail

    @staticmethod
    def __build_frames(reqs, config, spark):
        """
        Builds the dataframes provided in the requirements file.
        Returns them in a dictionary called frames.

        Args:
            reqs: requirements configuration in dictionary format
            config: the build configuration in dictionary format
            spark: the spark session

        Returns:
            frames: dictionary with all the dataframes
        """
        start = config["start"]
        end = config["end"]
        seasonality = config["seasonality"]
        frames = {}

        season_start = Loader.__seasonality_start(end, seasonality)
        print(reqs.items())
        for key, value in reqs.items():
            if value["type"] == "intermediate":
                # at present def frames look back 52 weeks, season 5 years (can be dynamically changed in yaml)
                # season frames, as you might expect,  are used for seasonality features
                frames[key] = {
                    "def": Loader.__load_intermediate(
                        value["path"], start, end, spark
                    ),
                    "season": Loader.__load_intermediate(
                        value["path"], season_start, end, spark
                    ),
                }
            elif value["type"] == "master":
                frames[key] = Loader.__load_master(value["path"], spark)
            elif value["type"] == "template":
                frames[key] = Loader.__load_template(value["path"], spark)
        if "item" in reqs:
            frames["detail"]["def"] = Loader.__add_brand_to_detail(
                frames["item"], frames["detail"]["def"]
            )
            frames["detail"]["season"] = Loader.__add_brand_to_detail(
                frames["item"], frames["detail"]["season"]
            )

        return frames

    @staticmethod
    def load(reqs, config):

        spark = Loader.spark
        frames = Loader.__build_frames(reqs, config, spark)
        for key in ["def", "season"]:
            frames["detail"][key] = Loader.__filter_detail(
                frames["detail"][key], key, frames["member_extended"], config
            )

        return frames
