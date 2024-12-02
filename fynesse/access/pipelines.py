from fynesse.common.db.db_setup import is_pipeline_in_progress, create_metadata_table_if_not_exists, \
    update_metadata_on_start_pipeline, update_metadata_on_end_pipeline
from fynesse.common.pipelines.get_indicators import init_get_indicators, resume_get_indicators
from fynesse.common.pipelines.part_1 import init_part_1
from fynesse.common.pipelines.part_1_nssec_msoa import init_part_1_nssec_msoa
from fynesse.common.pipelines.postcode import init_postcode
from fynesse.common.pipelines.price_paid import init_price_paid, resume_price_paid

PRICE_PAID_PIPELINE = "price_paid"
PART_1_PIPELINE = "part_1"
PART_1_NSSEC_MSOA_PIPELINE = "part_1_nssec_msoa"
GET_INDICATORS_OA = "get_indicators_oa"
GET_INDICATORS_MSOA = "get_indicators_msoa"
POSTCODE_PIPELINE = "postcode"


def restart_pipeline(connection, pipeline_name):
    create_metadata_table_if_not_exists(connection)

    in_progress, last_start_time, last_end_time = is_pipeline_in_progress(connection,
                                                                          PRICE_PAID_PIPELINE
                                                                          )
    if in_progress:
        print(
                f"Pipeline {pipeline_name} is in progress, and was started at {str(last_start_time)}"
        )
        print(f"Are you sure you want to restart?? This could potentially lose a lot of progress.")
        init_confirmation = input(f"Restart pipeline? y/n: ")
        if init_confirmation.strip().lower() in ["yes", "y"]:
            init_pipeline(connection, pipeline_name)
        else:
            print("Pipeline will not be restarted")
            resume_confirmation = input(
                    f"Resume pipeline? y/n: "
            )
            if resume_confirmation.strip().lower() in ["yes", "y"]:
                return
    else:
        init_pipeline(connection, pipeline_name)

    resume_pipeline(connection, pipeline_name, progress_check=False)


def init_pipeline(connection, pipeline_name):
    create_metadata_table_if_not_exists(connection)

    update_metadata_on_start_pipeline(connection, pipeline_name)

    if pipeline_name == PRICE_PAID_PIPELINE:
        init_price_paid(connection)

    if pipeline_name == PART_1_PIPELINE:
        init_part_1(connection)

    if pipeline_name == PART_1_NSSEC_MSOA_PIPELINE:
        init_part_1_nssec_msoa(connection)

    if pipeline_name == GET_INDICATORS_OA:
        init_get_indicators(connection, "nssec_oa_geog", "poi_counts_oa")

    if pipeline_name == GET_INDICATORS_MSOA:
        init_get_indicators(connection, "sexual_orientation_nssec_msoa_geog", "poi_counts_msoa")

    if pipeline_name == POSTCODE_PIPELINE:
        init_postcode(connection)


def resume_pipeline(connection, pipeline_name, progress_check=True):
    create_metadata_table_if_not_exists(connection)

    if progress_check:
        in_progress, last_start_time, last_end_time = is_pipeline_in_progress(connection,
                                                                              PRICE_PAID_PIPELINE
                                                                              )
        if not in_progress:
            print(f"Pipeline {pipeline_name} has not started.")
            print(f"Or was completed at {last_end_time}")
            init_confirmation = input(f"Restart pipeline? y/n: ")
            if init_confirmation.strip().lower() in ["yes", "y"]:
                init_pipeline(connection, pipeline_name)
            else:
                return

    if pipeline_name == PRICE_PAID_PIPELINE:
        resume_price_paid(connection)

    if pipeline_name == GET_INDICATORS_OA:
        resume_get_indicators(connection, "nssec_oa_geog", "poi_counts_oa")

    if pipeline_name == GET_INDICATORS_MSOA:
        resume_get_indicators(connection, "sexual_orientation_nssec_msoa_geog", "poi_counts_msoa")

    update_metadata_on_end_pipeline(connection, pipeline_name)
