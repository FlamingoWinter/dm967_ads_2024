from fynesse.common.db.db import run_query
from fynesse.common.db.db_index_management import add_index


def init_relate_postcode_price_paid(connection):
    run_query(connection, """UPDATE price_paid
    JOIN postcode
    ON price_paid.postcode = postcode.postcode
    SET price_paid.db_id = postcode.id;"""
              )
    add_index(connection, "price_paid", "db_id")
