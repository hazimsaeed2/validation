from memberdna.source_etl.utils.utility import *


def selectSampledMembers(spark, data_paths):
    member = spark.read.parquet(data_paths["intermediate"]["member"])
    print("member count: ", member.count())

    sampled_member = member.sample(False, 0.05, 3)
    print("sampled member count: ", sampled_member.count())

    sampled_member.write.parquet(
        data_paths["sampled"]["member"], mode="overwrite"
    )


def readSampledMemberIDs(spark, data_paths):
    sampled_member = spark.read.parquet(data_paths["sampled"]["member"])
    sampled_member = sampled_member.select("MBRSHP_SID")
    sampled_member.cache()
    return sampled_member


def sampleIntermediate(spark, intermediate_name, sampled_member, data_paths):
    df = spark.read.parquet(data_paths["intermediate"][intermediate_name])
    sampled_df = df.join(sampled_member, "MBRSHP_SID")
    print(intermediate_name, df.count(), sampled_df.count())

    if "FISCAL_WEEK_END" in sampled_df.schema.names:
        sampled_df.repartition("FISCAL_WEEK_END").write.parquet(
            data_paths["sampled"][intermediate_name],
            partitionBy="FISCAL_WEEK_END",
            mode="overwrite",
        )
    else:
        sampled_df.write.parquet(
            data_paths["sampled"][intermediate_name], mode="overwrite"
        )


def main(spark, data_paths):

    # running the line directly below gets a new sample of members. if you want continuity in the sample (same members) comment out next line
    selectSampledMembers(spark, data_paths)

    sampled_member = readSampledMemberIDs(spark, data_paths)

    non_member_intermediates = {
        intermediate_name
        for intermediate_name in data_paths["sampled"]
        if intermediate_name != "member"
    }
    for intermediate_name in non_member_intermediates:
        print("sampling ", intermediate_name)
        sampleIntermediate(
            spark, intermediate_name, sampled_member, data_paths
        )
