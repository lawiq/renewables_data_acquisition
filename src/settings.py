from datetime import datetime

EIA_API_KEY = "8f7af15657106a9e3101178ff1d9999c"
EIA_ARCHIVE_URL = "https://www.eia.gov/electricity/data/eia860m/archive/xls/{}_generator{}.xlsx"
EIA_RECENT_URL = "https://www.eia.gov/electricity/data/eia860m/xls/{}_generator{}.xlsx"

FIELDS = [
    'unique_id',
    'entity_id',
    'entity_name',
    'unit_code',
    'sector',
    'plant_state',
    'nameplate_capacity_mw',
    'net_summer_capacity_mw',
    'net_winter_capacity_mw',
    'technology',
    'energy_source_code',
    'prime_mover_code',
    'og_planned_operation_month',
    'cur_planned_operation_month',
    'og_planned_operation_year',
    'cur_planned_operation_year',
    'planned_operation_delta_months',
    'operating_month',
    'operating_year',
    'planned_retirement_month',
    'planned_retirement_year',
    'retirement_month',
    'retirement_year',
    'status',
    'status_p_start',
    'status_p_end',
    'status_l_start',
    'status_l_end',
    'status_t_start',
    'status_t_end',
    'status_u_start',
    'status_u_end',
    'status_v_start',
    'status_v_end',
    'status_ts_start',
    'status_ts_end',
    'status_other_start',
    'status_other_end',
    'county',
    'latitude',
    'longitude',
    'balancing_authority_code',
    'initial_report_status',
    'initial_report_date'
]

SKIP_ROW_MAPPING = {
    2015: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6:1, 7:1, 8: 1, 9: 1, 10: 1, 11: 1, 12: 1},
    2016: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6:1, 7:1, 8: 1, 9: 1, 10: 1, 11: 1, 12: 1},
    2017: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6:1, 7:1, 8: 1, 9: 1, 10: 1, 11: 1, 12: 1},
    2018: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6:1, 7:1, 8: 1, 9: 1, 10: 1, 11: 1, 12: 1},
    2019: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6:1, 7:1, 8: 1, 9: 1, 10: 1, 11: 1, 12: 1},
    2020: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6:1, 7:1, 8: 1, 9: 1, 10: 1, 11: 2, 12: 2},
    2021: {1: 2, 2: 2, 3: 2}
}

SHEETS = [
    'Operating',
    'Planned',
    'Retired',
    'Canceled or Postponed'
]
