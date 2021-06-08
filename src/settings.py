from datetime import datetime

EIA_API_KEY = "8f7af15657106a9e3101178ff1d9999c"
EIA_ARCHIVE_URL = "https://www.eia.gov/electricity/data/eia860m/archive/xls/{}_generator{}.xlsx"
EIA_RECENT_URL = "https://www.eia.gov/electricity/data/eia860m/xls/{}_generator{}.xlsx"

FIELDS = [
    'Entity ID',
    'Entity Name',
    'Sector',
    'Plant State',
    'Nameplate Capacity (MW)',
    'Net Summer Capacity (MW)',
    'Net Winter Capacity (MW)',
    'Technology',
    'Energy Source Code',
    'Prime Mover Code',
    'OG Planned Operation Month',
    'Cur Planned Operation Month',
    'OG Planned Operation Year',
    'Cur Planned Operation Year',
    'Planned Operation Delta (months)',
    'Operating Month',
    'Operating Year',
    'Planned Retirement Month',
    'Planned Retirement Year',
    'Retirement Month',
    'Retirement Year',
    'Status',
    'Status P Start',
    'Status P End',
    'Status L Start',
    'Status L End',
    'Status T Start',
    'Status T End',
    'Status U Start',
    'Status U End',
    'Status V Start',
    'Status V End',
    'Status TS Start',
    'Status TS End',
    'Status Other Start',
    'Status Other End',
    'County',
    'Latitude',
    'Longitude',
    'Balancing Authority Code',
    'Initial Status', # the status when the project first reported
    'Initial Date', # when the project first reported
]

SKIP_ROW_MAPPING = {
    2015: 1,
    2016: 1,
    2017: 1,
    2018: 1,
    2019: 1,
    2020: 1,
    2021: 2
}

SHEETS = [
    'Operating',
    'Planned',
    'Retired',
    'Canceled or Postponed'
]
