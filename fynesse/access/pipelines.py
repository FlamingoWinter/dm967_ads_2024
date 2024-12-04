from fynesse.common.db.db_setup import is_pipeline_in_progress, create_metadata_table_if_not_exists, \
    update_metadata_on_start_pipeline, update_metadata_on_end_pipeline
from fynesse.common.pipelines.add_census_data import init_add_census_data
from fynesse.common.pipelines.beach import init_beach_pipeline
from fynesse.common.pipelines.beach_intersects_msoa import init_beach_intersects_msoa
from fynesse.common.pipelines.get_indicators import init_get_indicators, resume_get_indicators
from fynesse.common.pipelines.part_1 import init_part_1
from fynesse.common.pipelines.part_1_nssec_msoa import init_part_1_nssec_msoa
from fynesse.common.pipelines.postcode import init_postcode
from fynesse.common.pipelines.price_paid import init_price_paid, resume_price_paid
from fynesse.common.pipelines.process_postcode import init_process_postcodes, \
    resume_process_postcodes
from fynesse.common.pipelines.relate_postcode_price_paid import init_relate_postcode_price_paid
from fynesse.common.pipelines.upload_work_type_relationships import \
    init_upload_work_type_relationships

PRICE_PAID_PIPELINE = "price_paid"
PART_1_PIPELINE = "part_1"
PART_1_NSSEC_MSOA_PIPELINE = "part_1_nssec_msoa"
GET_INDICATORS_OA = "get_indicators_oa"
GET_INDICATORS_MSOA = "get_indicators_msoa"
POSTCODE_PIPELINE = "postcode"
ADD_CENSUS_DATA_PIPELINE = "add_census_data"
BEACH_PIPELINE = "beach"
BEACH_INTERSECTS_MSOA = "beach_intersects_msoa"
PROCESS_POSTCODES = "process_postcodes"
UPLOAD_WORK_TYPE_RELATIONS = "upload_work_type_relationships"
RELATE_POSTCODE_PRICE_PAID_PIPELINE = "relate_postcode_price_paid"


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

    if pipeline_name == ADD_CENSUS_DATA_PIPELINE:
        init_add_census_data(connection)

    if pipeline_name == BEACH_PIPELINE:
        init_beach_pipeline(connection)

    if pipeline_name == BEACH_INTERSECTS_MSOA:
        init_beach_intersects_msoa(connection)

    if pipeline_name == PROCESS_POSTCODES:
        init_process_postcodes(connection)

    if pipeline_name == UPLOAD_WORK_TYPE_RELATIONS:
        init_upload_work_type_relationships(connection)

    if pipeline_name == RELATE_POSTCODE_PRICE_PAID_PIPELINE:
        init_relate_postcode_price_paid(connection)


def resume_pipeline(connection, pipeline_name, progress_check=True):
    create_metadata_table_if_not_exists(connection)

    if progress_check:
        in_progress, last_start_time, last_end_time = is_pipeline_in_progress(connection,
                                                                              pipeline_name
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

    if pipeline_name == PROCESS_POSTCODES:
        resume_process_postcodes(connection)

    update_metadata_on_end_pipeline(connection, pipeline_name)
