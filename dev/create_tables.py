import pyarrow
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, LongType, DecimalType, \
    DateType, StringType

catalog = RestCatalog(
    "rest",
    **{
        "uri": "http://localhost:8181"
    }
)

catalog.create_namespace_if_not_exists("tpc_ds")

catalog.create_table_if_not_exists(
    identifier='tpc_ds.call_center',
    schema=Schema(
        NestedField(field_id=0, name='cc_call_center_sk', field_type=IntegerType(), required=True),
        NestedField(field_id=1, name='cc_call_center_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='cc_rec_start_date', field_type=DateType(), required=False),
        NestedField(field_id=3, name='cc_rec_end_date', field_type=DateType(), required=False),
        NestedField(field_id=4, name='cc_closed_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=5, name='cc_open_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=6, name='cc_name', field_type=StringType(), required=True),
        NestedField(field_id=7, name='cc_class', field_type=StringType(), required=True),
        NestedField(field_id=8, name='cc_employees', field_type=IntegerType(), required=True),
        NestedField(field_id=9, name='cc_sq_ft', field_type=IntegerType(), required=True),
        NestedField(field_id=10, name='cc_hours', field_type=StringType(), required=True),
        NestedField(field_id=11, name='cc_manager', field_type=StringType(), required=True),
        NestedField(field_id=12, name='cc_mkt_id', field_type=IntegerType(), required=True),
        NestedField(field_id=13, name='cc_mkt_class', field_type=StringType(), required=True),
        NestedField(field_id=14, name='cc_mkt_desc', field_type=StringType(), required=True),
        NestedField(field_id=15, name='cc_market_manager', field_type=StringType(), required=True),
        NestedField(field_id=16, name='cc_division', field_type=IntegerType(), required=True),
        NestedField(field_id=17, name='cc_division_name', field_type=StringType(), required=True),
        NestedField(field_id=18, name='cc_company', field_type=IntegerType(), required=True),
        NestedField(field_id=19, name='cc_company_name', field_type=StringType(), required=True),
        NestedField(field_id=20, name='cc_street_number', field_type=StringType(), required=True),
        NestedField(field_id=21, name='cc_street_name', field_type=StringType(), required=True),
        NestedField(field_id=22, name='cc_street_type', field_type=StringType(), required=True),
        NestedField(field_id=23, name='cc_suite_number', field_type=StringType(), required=True),
        NestedField(field_id=24, name='cc_city', field_type=StringType(), required=True),
        NestedField(field_id=25, name='cc_county', field_type=StringType(), required=True),
        NestedField(field_id=26, name='cc_state', field_type=StringType(), required=True),
        NestedField(field_id=27, name='cc_zip', field_type=StringType(), required=True),
        NestedField(field_id=28, name='cc_country', field_type=StringType(), required=True),
        NestedField(field_id=29, name='cc_gmt_offset', field_type=DecimalType(7, 2), required=True),
        NestedField(field_id=30, name='cc_tax_percentage', field_type=DecimalType(7, 2), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.catalog_page',
    schema=Schema(
        NestedField(field_id=0, name='cp_catalog_page_sk', field_type=IntegerType(), required=True),
        NestedField(field_id=1, name='cp_catalog_page_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='cp_start_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=3, name='cp_end_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=4, name='cp_department', field_type=StringType(), required=False),
        NestedField(field_id=5, name='cp_catalog_number', field_type=IntegerType(), required=False),
        NestedField(field_id=6, name='cp_catalog_page_number', field_type=IntegerType(), required=False),
        NestedField(field_id=7, name='cp_description', field_type=StringType(), required=False),
        NestedField(field_id=8, name='cp_type', field_type=StringType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.catalog_returns',
    schema=Schema(
        NestedField(field_id=0, name='cr_returned_date_sk', field_type=IntegerType(), required=True),
        NestedField(field_id=1, name='cr_returned_time_sk', field_type=LongType(), required=True),
        NestedField(field_id=2, name='cr_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=3, name='cr_refunded_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=4, name='cr_refunded_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='cr_refunded_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=6, name='cr_refunded_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=7, name='cr_returning_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=8, name='cr_returning_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=9, name='cr_returning_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=10, name='cr_returning_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=11, name='cr_call_center_sk', field_type=LongType(), required=False),
        NestedField(field_id=12, name='cr_catalog_page_sk', field_type=LongType(), required=False),
        NestedField(field_id=13, name='cr_ship_mode_sk', field_type=LongType(), required=False),
        NestedField(field_id=14, name='cr_warehouse_sk', field_type=LongType(), required=False),
        NestedField(field_id=15, name='cr_reason_sk', field_type=LongType(), required=False),
        NestedField(field_id=16, name='cr_order_number', field_type=LongType(), required=True),
        NestedField(field_id=17, name='cr_return_quantity', field_type=IntegerType(), required=False),
        NestedField(field_id=18, name='cr_return_amount', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=19, name='cr_return_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=20, name='cr_return_amt_inc_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=21, name='cr_fee', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=22, name='cr_return_ship_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=23, name='cr_refunded_cash', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=24, name='cr_reversed_charge', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=25, name='cr_store_credit', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=26, name='cr_net_loss', field_type=DecimalType(7, 2), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.catalog_sales',
    schema=Schema(
        NestedField(field_id=0, name='cs_sold_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=1, name='cs_sold_time_sk', field_type=LongType(), required=False),
        NestedField(field_id=2, name='cs_ship_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=3, name='cs_bill_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=4, name='cs_bill_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='cs_bill_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=6, name='cs_bill_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=7, name='cs_ship_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=8, name='cs_ship_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=9, name='cs_ship_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=10, name='cs_ship_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=11, name='cs_call_center_sk', field_type=LongType(), required=False),
        NestedField(field_id=12, name='cs_catalog_page_sk', field_type=LongType(), required=False),
        NestedField(field_id=13, name='cs_ship_mode_sk', field_type=LongType(), required=False),
        NestedField(field_id=14, name='cs_warehouse_sk', field_type=LongType(), required=False),
        NestedField(field_id=15, name='cs_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=16, name='cs_promo_sk', field_type=LongType(), required=False),
        NestedField(field_id=17, name='cs_order_number', field_type=LongType(), required=True),
        NestedField(field_id=18, name='cs_quantity', field_type=IntegerType(), required=False),
        NestedField(field_id=19, name='cs_wholesale_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=20, name='cs_list_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=21, name='cs_sales_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=22, name='cs_ext_discount_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=23, name='cs_ext_sales_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=24, name='cs_ext_wholesale_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=25, name='cs_ext_list_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=26, name='cs_ext_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=27, name='cs_coupon_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=28, name='cs_ext_ship_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=29, name='cs_net_paid', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=30, name='cs_net_paid_inc_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=31, name='cs_net_paid_inc_ship', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=32, name='cs_net_paid_inc_ship_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=33, name='cs_net_profit', field_type=DecimalType(7, 2), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.customer_address',
    schema=Schema(
        NestedField(field_id=0, name='ca_address_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='ca_address_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='ca_street_number', field_type=StringType(), required=False),
        NestedField(field_id=3, name='ca_street_name', field_type=StringType(), required=False),
        NestedField(field_id=4, name='ca_street_type', field_type=StringType(), required=False),
        NestedField(field_id=5, name='ca_suite_number', field_type=StringType(), required=False),
        NestedField(field_id=6, name='ca_city', field_type=StringType(), required=False),
        NestedField(field_id=7, name='ca_county', field_type=StringType(), required=False),
        NestedField(field_id=8, name='ca_state', field_type=StringType(), required=False),
        NestedField(field_id=9, name='ca_zip', field_type=StringType(), required=False),
        NestedField(field_id=10, name='ca_country', field_type=StringType(), required=False),
        NestedField(field_id=11, name='ca_gmt_offset', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=12, name='ca_location_type', field_type=StringType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.customer_demographics',
    schema=Schema(
        NestedField(field_id=0, name='cd_demo_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='cd_gender', field_type=StringType(), required=True),
        NestedField(field_id=2, name='cd_marital_status', field_type=StringType(), required=True),
        NestedField(field_id=3, name='cd_education_status', field_type=StringType(), required=True),
        NestedField(field_id=4, name='cd_purchase_estimate', field_type=IntegerType(), required=True),
        NestedField(field_id=5, name='cd_credit_rating', field_type=StringType(), required=True),
        NestedField(field_id=6, name='cd_dep_count', field_type=IntegerType(), required=True),
        NestedField(field_id=7, name='cd_dep_employed_count', field_type=IntegerType(), required=True),
        NestedField(field_id=8, name='cd_dep_college_count', field_type=IntegerType(), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.customer',
    schema=Schema(
        NestedField(field_id=0, name='c_customer_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='c_customer_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='c_current_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=3, name='c_current_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=4, name='c_current_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='c_first_shipto_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=6, name='c_first_sales_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=7, name='c_salutation', field_type=StringType(), required=False),
        NestedField(field_id=8, name='c_first_name', field_type=StringType(), required=False),
        NestedField(field_id=9, name='c_last_name', field_type=StringType(), required=False),
        NestedField(field_id=10, name='c_preferred_cust_flag', field_type=StringType(), required=False),
        NestedField(field_id=11, name='c_birth_day', field_type=IntegerType(), required=False),
        NestedField(field_id=12, name='c_birth_month', field_type=IntegerType(), required=False),
        NestedField(field_id=13, name='c_birth_year', field_type=IntegerType(), required=False),
        NestedField(field_id=14, name='c_birth_country', field_type=StringType(), required=False),
        NestedField(field_id=15, name='c_login', field_type=StringType(), required=False),
        NestedField(field_id=16, name='c_email_address', field_type=StringType(), required=False),
        NestedField(field_id=17, name='c_last_review_date', field_type=StringType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.date_dim',
    schema=Schema(
        NestedField(field_id=0, name='d_date_sk', field_type=IntegerType(), required=True),
        NestedField(field_id=1, name='d_date_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='d_date', field_type=DateType(), required=True),
        NestedField(field_id=3, name='d_month_seq', field_type=IntegerType(), required=True),
        NestedField(field_id=4, name='d_week_seq', field_type=IntegerType(), required=True),
        NestedField(field_id=5, name='d_quarter_seq', field_type=IntegerType(), required=True),
        NestedField(field_id=6, name='d_year', field_type=IntegerType(), required=True),
        NestedField(field_id=7, name='d_dow', field_type=IntegerType(), required=True),
        NestedField(field_id=8, name='d_moy', field_type=IntegerType(), required=True),
        NestedField(field_id=9, name='d_dom', field_type=IntegerType(), required=True),
        NestedField(field_id=10, name='d_qoy', field_type=IntegerType(), required=True),
        NestedField(field_id=11, name='d_fy_year', field_type=IntegerType(), required=True),
        NestedField(field_id=12, name='d_fy_quarter_seq', field_type=IntegerType(), required=True),
        NestedField(field_id=13, name='d_fy_week_seq', field_type=IntegerType(), required=True),
        NestedField(field_id=14, name='d_day_name', field_type=StringType(), required=True),
        NestedField(field_id=15, name='d_quarter_name', field_type=StringType(), required=True),
        NestedField(field_id=16, name='d_holiday', field_type=StringType(), required=True),
        NestedField(field_id=17, name='d_weekend', field_type=StringType(), required=True),
        NestedField(field_id=18, name='d_following_holiday', field_type=StringType(), required=True),
        NestedField(field_id=19, name='d_first_dom', field_type=IntegerType(), required=True),
        NestedField(field_id=20, name='d_last_dom', field_type=IntegerType(), required=True),
        NestedField(field_id=21, name='d_same_day_ly', field_type=IntegerType(), required=True),
        NestedField(field_id=22, name='d_same_day_lq', field_type=IntegerType(), required=True),
        NestedField(field_id=23, name='d_current_day', field_type=StringType(), required=True),
        NestedField(field_id=24, name='d_current_week', field_type=StringType(), required=True),
        NestedField(field_id=25, name='d_current_month', field_type=StringType(), required=True),
        NestedField(field_id=26, name='d_current_quarter', field_type=StringType(), required=True),
        NestedField(field_id=27, name='d_current_year', field_type=StringType(), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.household_demographics',
    schema=Schema(
        NestedField(field_id=0, name='hd_demo_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='hd_income_band_sk', field_type=LongType(), required=True),
        NestedField(field_id=2, name='hd_buy_potential', field_type=StringType(), required=True),
        NestedField(field_id=3, name='hd_dep_count', field_type=IntegerType(), required=True),
        NestedField(field_id=4, name='hd_vehicle_count', field_type=IntegerType(), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.income_band',
    schema=Schema(
        NestedField(field_id=0, name='ib_income_band_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='ib_lower_bound', field_type=IntegerType(), required=True),
        NestedField(field_id=2, name='ib_upper_bound', field_type=IntegerType(), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.inventory',
    schema=Schema(
        NestedField(field_id=0, name='inv_date_sk', field_type=IntegerType(), required=True),
        NestedField(field_id=1, name='inv_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=2, name='inv_warehouse_sk', field_type=LongType(), required=True),
        NestedField(field_id=3, name='inv_quantity_on_hand', field_type=IntegerType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.item',
    schema=Schema(
        NestedField(field_id=0, name='i_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='i_item_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='i_rec_start_date', field_type=StringType(), required=False),
        NestedField(field_id=3, name='i_rec_end_date', field_type=StringType(), required=False),
        NestedField(field_id=4, name='i_item_desc', field_type=StringType(), required=False),
        NestedField(field_id=5, name='i_current_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=6, name='i_wholesale_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=7, name='i_brand_id', field_type=IntegerType(), required=False),
        NestedField(field_id=8, name='i_brand', field_type=StringType(), required=False),
        NestedField(field_id=9, name='i_class_id', field_type=IntegerType(), required=False),
        NestedField(field_id=10, name='i_class', field_type=StringType(), required=False),
        NestedField(field_id=11, name='i_category_id', field_type=IntegerType(), required=False),
        NestedField(field_id=12, name='i_category', field_type=StringType(), required=False),
        NestedField(field_id=13, name='i_manufact_id', field_type=IntegerType(), required=False),
        NestedField(field_id=14, name='i_manufact', field_type=StringType(), required=False),
        NestedField(field_id=15, name='i_size', field_type=StringType(), required=False),
        NestedField(field_id=16, name='i_formulation', field_type=StringType(), required=False),
        NestedField(field_id=17, name='i_color', field_type=StringType(), required=False),
        NestedField(field_id=18, name='i_units', field_type=StringType(), required=False),
        NestedField(field_id=19, name='i_container', field_type=StringType(), required=False),
        NestedField(field_id=20, name='i_manager_id', field_type=IntegerType(), required=False),
        NestedField(field_id=21, name='i_product_name', field_type=StringType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.promotion',
    schema=Schema(
        NestedField(field_id=0, name='p_promo_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='p_promo_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='p_start_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=3, name='p_end_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=4, name='p_item_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='p_cost', field_type=DecimalType(15, 2), required=False),
        NestedField(field_id=6, name='p_response_target', field_type=IntegerType(), required=False),
        NestedField(field_id=7, name='p_promo_name', field_type=StringType(), required=False),
        NestedField(field_id=8, name='p_channel_dmail', field_type=StringType(), required=False),
        NestedField(field_id=9, name='p_channel_email', field_type=StringType(), required=False),
        NestedField(field_id=10, name='p_channel_catalog', field_type=StringType(), required=False),
        NestedField(field_id=11, name='p_channel_tv', field_type=StringType(), required=False),
        NestedField(field_id=12, name='p_channel_radio', field_type=StringType(), required=False),
        NestedField(field_id=13, name='p_channel_press', field_type=StringType(), required=False),
        NestedField(field_id=14, name='p_channel_event', field_type=StringType(), required=False),
        NestedField(field_id=15, name='p_channel_demo', field_type=StringType(), required=False),
        NestedField(field_id=16, name='p_channel_details', field_type=StringType(), required=False),
        NestedField(field_id=17, name='p_purpose', field_type=StringType(), required=False),
        NestedField(field_id=18, name='p_discount_active', field_type=StringType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.reason',
    schema=Schema(
        NestedField(field_id=0, name='r_reason_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='r_reason_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='r_reason_desc', field_type=StringType(), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.ship_mode',
    schema=Schema(
        NestedField(field_id=0, name='sm_ship_mode_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='sm_ship_mode_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='sm_type', field_type=StringType(), required=True),
        NestedField(field_id=3, name='sm_code', field_type=StringType(), required=True),
        NestedField(field_id=4, name='sm_carrier', field_type=StringType(), required=True),
        NestedField(field_id=5, name='sm_contract', field_type=StringType(), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.store_returns',
    schema=Schema(
        NestedField(field_id=0, name='sr_returned_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=1, name='sr_return_time_sk', field_type=LongType(), required=False),
        NestedField(field_id=2, name='sr_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=3, name='sr_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=4, name='sr_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='sr_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=6, name='sr_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=7, name='sr_store_sk', field_type=LongType(), required=False),
        NestedField(field_id=8, name='sr_reason_sk', field_type=LongType(), required=False),
        NestedField(field_id=9, name='sr_ticket_number', field_type=LongType(), required=True),
        NestedField(field_id=10, name='sr_return_quantity', field_type=IntegerType(), required=False),
        NestedField(field_id=11, name='sr_return_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=12, name='sr_return_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=13, name='sr_return_amt_inc_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=14, name='sr_fee', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=15, name='sr_return_ship_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=16, name='sr_refunded_cash', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=17, name='sr_reversed_charge', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=18, name='sr_store_credit', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=19, name='sr_net_loss', field_type=DecimalType(7, 2), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.store_sales',
    schema=Schema(
        NestedField(field_id=0, name='ss_sold_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=1, name='ss_sold_time_sk', field_type=LongType(), required=False),
        NestedField(field_id=2, name='ss_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=3, name='ss_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=4, name='ss_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='ss_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=6, name='ss_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=7, name='ss_store_sk', field_type=LongType(), required=False),
        NestedField(field_id=8, name='ss_promo_sk', field_type=LongType(), required=False),
        NestedField(field_id=9, name='ss_ticket_number', field_type=LongType(), required=True),
        NestedField(field_id=10, name='ss_quantity', field_type=IntegerType(), required=False),
        NestedField(field_id=11, name='ss_wholesale_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=12, name='ss_list_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=13, name='ss_sales_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=14, name='ss_ext_discount_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=15, name='ss_ext_sales_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=16, name='ss_ext_wholesale_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=17, name='ss_ext_list_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=18, name='ss_ext_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=19, name='ss_coupon_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=20, name='ss_net_paid', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=21, name='ss_net_paid_inc_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=22, name='ss_net_profit', field_type=DecimalType(7, 2), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.store',
    schema=Schema(
        NestedField(field_id=0, name='s_store_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='s_store_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='s_rec_start_date', field_type=StringType(), required=False),
        NestedField(field_id=3, name='s_rec_end_date', field_type=StringType(), required=False),
        NestedField(field_id=4, name='s_closed_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=5, name='s_store_name', field_type=StringType(), required=False),
        NestedField(field_id=6, name='s_number_employees', field_type=IntegerType(), required=False),
        NestedField(field_id=7, name='s_floor_space', field_type=IntegerType(), required=False),
        NestedField(field_id=8, name='s_hours', field_type=StringType(), required=False),
        NestedField(field_id=9, name='s_manager', field_type=StringType(), required=False),
        NestedField(field_id=10, name='s_market_id', field_type=IntegerType(), required=False),
        NestedField(field_id=11, name='s_geography_class', field_type=StringType(), required=False),
        NestedField(field_id=12, name='s_market_desc', field_type=StringType(), required=False),
        NestedField(field_id=13, name='s_market_manager', field_type=StringType(), required=False),
        NestedField(field_id=14, name='s_division_id', field_type=IntegerType(), required=False),
        NestedField(field_id=15, name='s_division_name', field_type=StringType(), required=False),
        NestedField(field_id=16, name='s_company_id', field_type=IntegerType(), required=False),
        NestedField(field_id=17, name='s_company_name', field_type=StringType(), required=False),
        NestedField(field_id=18, name='s_street_number', field_type=StringType(), required=False),
        NestedField(field_id=19, name='s_street_name', field_type=StringType(), required=False),
        NestedField(field_id=20, name='s_street_type', field_type=StringType(), required=False),
        NestedField(field_id=21, name='s_suite_number', field_type=StringType(), required=False),
        NestedField(field_id=22, name='s_city', field_type=StringType(), required=False),
        NestedField(field_id=23, name='s_county', field_type=StringType(), required=False),
        NestedField(field_id=24, name='s_state', field_type=StringType(), required=False),
        NestedField(field_id=25, name='s_zip', field_type=StringType(), required=False),
        NestedField(field_id=26, name='s_country', field_type=StringType(), required=False),
        NestedField(field_id=27, name='s_gmt_offset', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=28, name='s_tax_precentage', field_type=DecimalType(7, 2), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.time_dim',
    schema=Schema(
        NestedField(field_id=0, name='t_time_sk', field_type=IntegerType(), required=True),
        NestedField(field_id=1, name='t_time_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='t_time', field_type=IntegerType(), required=True),
        NestedField(field_id=3, name='t_hour', field_type=IntegerType(), required=True),
        NestedField(field_id=4, name='t_minute', field_type=IntegerType(), required=True),
        NestedField(field_id=5, name='t_second', field_type=IntegerType(), required=True),
        NestedField(field_id=6, name='t_am_pm', field_type=StringType(), required=True),
        NestedField(field_id=7, name='t_shift', field_type=StringType(), required=True),
        NestedField(field_id=8, name='t_sub_shift', field_type=StringType(), required=True),
        NestedField(field_id=9, name='t_meal_time', field_type=StringType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.warehouse',
    schema=Schema(
        NestedField(field_id=0, name='w_warehouse_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='w_warehouse_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='w_warehouse_name', field_type=StringType(), required=False),
        NestedField(field_id=3, name='w_warehouse_sq_ft', field_type=IntegerType(), required=False),
        NestedField(field_id=4, name='w_street_number', field_type=StringType(), required=False),
        NestedField(field_id=5, name='w_street_name', field_type=StringType(), required=False),
        NestedField(field_id=6, name='w_street_type', field_type=StringType(), required=False),
        NestedField(field_id=7, name='w_suite_number', field_type=StringType(), required=False),
        NestedField(field_id=8, name='w_city', field_type=StringType(), required=False),
        NestedField(field_id=9, name='w_county', field_type=StringType(), required=False),
        NestedField(field_id=10, name='w_state', field_type=StringType(), required=False),
        NestedField(field_id=11, name='w_zip', field_type=StringType(), required=False),
        NestedField(field_id=12, name='w_country', field_type=StringType(), required=False),
        NestedField(field_id=13, name='w_gmt_offset', field_type=DecimalType(7, 2), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.web_page',
    schema=Schema(
        NestedField(field_id=0, name='wp_web_page_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='wp_web_page_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='wp_rec_start_date', field_type=StringType(), required=False),
        NestedField(field_id=3, name='wp_rec_end_date', field_type=StringType(), required=False),
        NestedField(field_id=4, name='wp_creation_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=5, name='wp_access_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=6, name='wp_autogen_flag', field_type=StringType(), required=False),
        NestedField(field_id=7, name='wp_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=8, name='wp_url', field_type=StringType(), required=False),
        NestedField(field_id=9, name='wp_type', field_type=StringType(), required=False),
        NestedField(field_id=10, name='wp_char_count', field_type=IntegerType(), required=False),
        NestedField(field_id=11, name='wp_link_count', field_type=IntegerType(), required=False),
        NestedField(field_id=12, name='wp_image_count', field_type=IntegerType(), required=False),
        NestedField(field_id=13, name='wp_max_ad_count', field_type=IntegerType(), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.web_returns',
    schema=Schema(
        NestedField(field_id=0, name='wr_returned_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=1, name='wr_returned_time_sk', field_type=LongType(), required=False),
        NestedField(field_id=2, name='wr_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=3, name='wr_refunded_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=4, name='wr_refunded_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='wr_refunded_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=6, name='wr_refunded_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=7, name='wr_returning_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=8, name='wr_returning_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=9, name='wr_returning_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=10, name='wr_returning_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=11, name='wr_web_page_sk', field_type=LongType(), required=False),
        NestedField(field_id=12, name='wr_reason_sk', field_type=LongType(), required=False),
        NestedField(field_id=13, name='wr_order_number', field_type=LongType(), required=True),
        NestedField(field_id=14, name='wr_return_quantity', field_type=IntegerType(), required=False),
        NestedField(field_id=15, name='wr_return_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=16, name='wr_return_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=17, name='wr_return_amt_inc_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=18, name='wr_fee', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=19, name='wr_return_ship_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=20, name='wr_refunded_cash', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=21, name='wr_reversed_charge', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=22, name='wr_account_credit', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=23, name='wr_net_loss', field_type=DecimalType(7, 2), required=False),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.web_sales',
    schema=Schema(
        NestedField(field_id=0, name='ws_sold_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=1, name='ws_sold_time_sk', field_type=LongType(), required=False),
        NestedField(field_id=2, name='ws_ship_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=3, name='ws_item_sk', field_type=LongType(), required=True),
        NestedField(field_id=4, name='ws_bill_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=5, name='ws_bill_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=6, name='ws_bill_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=7, name='ws_bill_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=8, name='ws_ship_customer_sk', field_type=LongType(), required=False),
        NestedField(field_id=9, name='ws_ship_cdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=10, name='ws_ship_hdemo_sk', field_type=LongType(), required=False),
        NestedField(field_id=11, name='ws_ship_addr_sk', field_type=LongType(), required=False),
        NestedField(field_id=12, name='ws_web_page_sk', field_type=LongType(), required=False),
        NestedField(field_id=13, name='ws_web_site_sk', field_type=LongType(), required=False),
        NestedField(field_id=14, name='ws_ship_mode_sk', field_type=LongType(), required=False),
        NestedField(field_id=15, name='ws_warehouse_sk', field_type=LongType(), required=False),
        NestedField(field_id=16, name='ws_promo_sk', field_type=LongType(), required=False),
        NestedField(field_id=17, name='ws_order_number', field_type=LongType(), required=True),
        NestedField(field_id=18, name='ws_quantity', field_type=IntegerType(), required=False),
        NestedField(field_id=19, name='ws_wholesale_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=20, name='ws_list_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=21, name='ws_sales_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=22, name='ws_ext_discount_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=23, name='ws_ext_sales_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=24, name='ws_ext_wholesale_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=25, name='ws_ext_list_price', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=26, name='ws_ext_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=27, name='ws_coupon_amt', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=28, name='ws_ext_ship_cost', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=29, name='ws_net_paid', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=30, name='ws_net_paid_inc_tax', field_type=DecimalType(7, 2), required=False),
        NestedField(field_id=31, name='ws_net_paid_inc_ship', field_type=DecimalType(7, 2), required=True),
        NestedField(field_id=32, name='ws_net_paid_inc_ship_tax', field_type=DecimalType(7, 2), required=True),
        NestedField(field_id=33, name='ws_net_profit', field_type=DecimalType(7, 2), required=True),
    )
)

catalog.create_table_if_not_exists(
    identifier='tpc_ds.web_site',
    schema=Schema(
        NestedField(field_id=0, name='web_site_sk', field_type=LongType(), required=True),
        NestedField(field_id=1, name='web_site_id', field_type=StringType(), required=True),
        NestedField(field_id=2, name='web_rec_start_date', field_type=StringType(), required=True),
        NestedField(field_id=3, name='web_rec_end_date', field_type=StringType(), required=False),
        NestedField(field_id=4, name='web_name', field_type=StringType(), required=True),
        NestedField(field_id=5, name='web_open_date_sk', field_type=IntegerType(), required=True),
        NestedField(field_id=6, name='web_close_date_sk', field_type=IntegerType(), required=False),
        NestedField(field_id=7, name='web_class', field_type=StringType(), required=True),
        NestedField(field_id=8, name='web_manager', field_type=StringType(), required=True),
        NestedField(field_id=9, name='web_mkt_id', field_type=IntegerType(), required=True),
        NestedField(field_id=10, name='web_mkt_class', field_type=StringType(), required=True),
        NestedField(field_id=11, name='web_mkt_desc', field_type=StringType(), required=True),
        NestedField(field_id=12, name='web_market_manager', field_type=StringType(), required=True),
        NestedField(field_id=13, name='web_company_id', field_type=IntegerType(), required=True),
        NestedField(field_id=14, name='web_company_name', field_type=StringType(), required=True),
        NestedField(field_id=15, name='web_street_number', field_type=StringType(), required=True),
        NestedField(field_id=16, name='web_street_name', field_type=StringType(), required=True),
        NestedField(field_id=17, name='web_street_type', field_type=StringType(), required=True),
        NestedField(field_id=18, name='web_suite_number', field_type=StringType(), required=True),
        NestedField(field_id=19, name='web_city', field_type=StringType(), required=True),
        NestedField(field_id=20, name='web_county', field_type=StringType(), required=True),
        NestedField(field_id=21, name='web_state', field_type=StringType(), required=True),
        NestedField(field_id=22, name='web_zip', field_type=StringType(), required=True),
        NestedField(field_id=23, name='web_country', field_type=StringType(), required=True),
        NestedField(field_id=24, name='web_gmt_offset', field_type=DecimalType(7, 2), required=True),
        NestedField(field_id=25, name='web_tax_percentage', field_type=DecimalType(7, 2), required=True),
    )
)

catalog.create_namespace_if_not_exists("top_level_namespace")
catalog.create_table_if_not_exists(
    identifier='top_level_namespace.table1',
    schema=Schema(
        NestedField(field_id=0, name='id', field_type=LongType(), required=True),
    )
)


catalog.create_namespace_if_not_exists("tpc_ds.inner_namespace")
catalog.create_table_if_not_exists(
    identifier='tpc_ds.inner_namespace.table1',
    schema=Schema(
        NestedField(field_id=0, name='id', field_type=LongType(), required=True),
    )
)

catalog.create_namespace_if_not_exists("tpc_ds.inner_namespace.inner_namespace_deeper")
catalog.create_namespace_if_not_exists("a1")
catalog.create_namespace_if_not_exists("a1.b1")
catalog.create_namespace_if_not_exists("a1.b1.c1")


table = catalog.load_table(identifier='tpc_ds.catalog_page')
for i in range(1000):
    if i % 20 == 0:
        print(i)
    df = pyarrow.Table.from_pylist(
        [
            {"cp_catalog_page_sk": 1, "cp_catalog_page_id": "page1"},
            {"cp_catalog_page_sk": 2, "cp_catalog_page_id": "page2"},
            {"cp_catalog_page_sk": 3, "cp_catalog_page_id": "page3"},
            {"cp_catalog_page_sk": 4, "cp_catalog_page_id": "page4"},
            {"cp_catalog_page_sk": 5, "cp_catalog_page_id": "page5"},
        ], schema=table.schema().as_arrow()
    )
    table.append(df)

table.manage_snapshots().create_tag(table.snapshots()[40].snapshot_id, "v1.0").commit()
table.manage_snapshots().create_branch(table.snapshots()[40].snapshot_id, "staging").commit()
table.manage_snapshots().create_branch(table.snapshots()[87].snapshot_id, "staging2").commit()
