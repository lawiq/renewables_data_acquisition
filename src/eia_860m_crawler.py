import argparse
import calendar
import datetime
import logging

import requests
import pandas as pd

import settings


class Form860MCrawler():

    def __init__(self, args):
        self.start_month = args.start.month
        self.start_year = args.start.year

        self.end_month = args.end.month
        self.end_year = args.end.year

        month_iter, year_iter = self.start_month, self.start_year

        crawl_range = []

        end_date = datetime.datetime.strptime("{}-{}".format(self.end_month, self.end_year), "%m-%Y")
        start_date = datetime.datetime.strptime("{}-{}".format(self.start_month, self.start_year), "%m-%Y")
        while start_date <= end_date:
            crawl_range.append((calendar.month_name[month_iter], year_iter))
            if month_iter == 12:
                month_iter = 1
                year_iter += 1
            else:
                month_iter += 1
            start_date = start_date.replace(month=month_iter, year=year_iter)

        # while month_iter <= self.end_month and year_iter <= self.end_year:
        #     crawl_range.append((calendar.month_name[month_iter], year_iter))
        #     if month_iter == 12:
        #         month_iter = 1
        #         year_iter += 1
        #     else:
        #         month_iter += 1

        self.crawl_range = crawl_range
        print(crawl_range)

        self.master_df = pd.DataFrame(columns=settings.FIELDS)

    def crawl_filing(self, month, year):
        """
        Crawls the EIA for an 860M filing matching the given month and year,
        constructs and returns a single normalized dataframe.
        """
        archive_url = settings.EIA_ARCHIVE_URL
        recent_url = settings.EIA_RECENT_URL

        response = requests.get(archive_url.format(month.lower(), year))

        if response.status_code == 200:
            file_name = '{}_generator{}.xlsx'.format(month, year)
            with open(file_name, 'wb') as file:
                file.write(response.content)
            excel_file = pd.ExcelFile(file_name)
            skip_row_num = 1
            sheet_dfs = []
            for sheet in settings.SHEETS:
                temp_df = excel_file.parse(sheet, skiprows=skip_row_num)
                temp_df.drop(temp_df.tail(1).index, inplace=True)
                # strip the space out of column names, remove '(' and ')' change to lower case and replace space with underscore
                temp_df.rename(columns=lambda x: "_".join(x.strip().replace('(', '').replace(')', '').lower().split()), inplace=True)
                # change column name to "sector" from "sector_name"
                temp_df.rename(columns=lambda x: "sector" if x == "sector_name" else x, inplace=True)
                temp_df['sheet_type'] = sheet
                sheet_dfs.append(temp_df)

            full_df = pd.concat(sheet_dfs, axis=0, ignore_index=True)
            full_df['unique_id'] = full_df.apply(
                lambda row: str(int(row['plant_id'])) + '_' + str(row['plant_name']) + '_' + str(row['generator_id']),
                axis=1
            )
            full_df.set_index('unique_id', inplace=True)

            # Check that dataframe contains all needed columns, if not, add empty columns
            if 'unit_code' not in full_df.columns:
                full_df['unit_code'] = ""
            if 'nameplate_capacity_mw' not in full_df.columns:
                full_df['nameplate_capacity_mw'] = ""
            if 'net_winter_capacity_mw' not in full_df.columns:
                full_df['net_winter_capacity_mw'] = ""
            if 'county' not in full_df.columns:
                full_df['county'] = ""
            if 'latitude' not in full_df.columns:
                full_df['latitude'] = ""
            if 'longitude' not in full_df.columns:
                full_df['longitude'] = ""
            if 'balancing_authority_code' not in full_df.columns:
                full_df['balancing_authority_code'] = ""

            full_df[['planned_operation_month']] = full_df[['planned_operation_month']].fillna('')
            full_df[['planned_operation_year']] = full_df[['planned_operation_year']].fillna('')


        else:
            raise

        return full_df

    def update_master(self, filing_df, month, year):
        master_df = self.master_df
        for row in filing_df.itertuples():
            index = row[0]
            cur_status = row.status
            cur_sheet_type = row.sheet_type

            if index in master_df.index:
                old_status = master_df.at[index, 'status']

                master_df.at[index, 'entity_id'] = row.entity_id
                master_df.at[index, 'entity_name'] = row.entity_name
                master_df.at[index, 'sector'] = row.sector
                master_df.at[index, 'unit_code'] = row.unit_code
                master_df.at[index, 'plant_state'] = row.plant_state
                master_df.at[index, 'nameplate_capacity_mw'] = row.nameplate_capacity_mw
                master_df.at[index, 'net_summer_capacity_mw'] = row.net_summer_capacity_mw
                master_df.at[index, 'net_winter_capacity_mw'] = row.net_winter_capacity_mw
                master_df.at[index, 'technology'] = row.technology
                master_df.at[index, 'energy_source_code'] = row.energy_source_code
                master_df.at[index, 'prime_mover_code'] = row.prime_mover_code
                master_df.at[index, 'county'] = row.county
                master_df.at[index, 'latitude'] = row.latitude
                master_df.at[index, 'longitude'] = row.longitude
                master_df.at[index, 'balancing_authority_code'] = row.balancing_authority_code

                # Genrators are in service
                if cur_sheet_type == 'Operating':
                    master_df.at[index, 'operating_month'] = row.operating_month
                    master_df.at[index, 'operating_year'] = row.operating_year
                    master_df.at[index, 'planned_retirement_month'] = row.planned_retirement_month
                    master_df.at[index, 'planned_retirement_year'] = row.planned_retirement_year

                    if old_status != cur_status:
                        # change status
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'status_p_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'status_l_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'status_t_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'status_u_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'status_v_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'status_ts_end'] = str(month) + ' ' + str(year)
                        elif old_status == '(OT) Other':
                            master_df.at[index, 'status_other_end'] = str(month) + ' ' + str(year)
                        # other status changes will be statuses from the Opertaing, Retired, and Canceled or Postponed sheet => retired, canceled or postponed, (SB) Standby/Backup: available for service but not normally used, (OS) Out of service and NOT expected to return to service in next calendar year, (OA) Out of service but expected to return to service in next calendar year

                        master_df.at[index, 'status'] = cur_status


                elif cur_sheet_type == 'Planned':
                    if str(row.planned_operation_month).strip() and str(row.planned_operation_year).strip():
                        master_df.at[index, 'cur_planned_operation_month'] = row.planned_operation_month
                        master_df.at[index, 'cur_planned_operation_year'] = row.planned_operation_year

                        og_planned_operation_month = master_df.at[index, 'og_planned_operation_month']
                        og_planned_operation_year = master_df.at[index, 'og_planned_operation_year']

                        #calculate the difference in months
                        if str(og_planned_operation_month).strip() and str(og_planned_operation_year).strip():
                            end_date = datetime.datetime(int(row.planned_operation_year), int(row.planned_operation_month), 1)
                            start_date = datetime.datetime(int(og_planned_operation_year), int(og_planned_operation_month), 1)
                            diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                            master_df.at[index, 'planned_operation_delta_months'] = diff
                        # there is no original planned dates when it first reported, so adding to it
                        else:
                            master_df.at[index, 'og_planned_operation_year'] = row.planned_operation_year
                            master_df.at[index, 'og_planned_operation_month'] = row.planned_operation_month

                    # change of status
                    if old_status != row.status:
                        # if previous status is in '(P) Planned for installation, but regulatory approvals not initiated', then change of status means phase P has ended
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'status_p_end'] = str(month) + ' ' + str(year)
                            # if new status is in '(L) Regulatory approvals pending. Not under construction', then that means phase L has started
                            if cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'status_l_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'status_t_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'status_u_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'status_v_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'status_ts_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'status_other_start'] = str(month) + ' ' + str(year)

                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'status_l_end'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'status_p_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'status_t_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'status_u_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'status_v_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'status_ts_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'status_other_start'] = str(month) + ' ' + str(year)

                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'status_t_end'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'status_p_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'status_l_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'status_u_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'status_v_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'status_ts_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'status_other_start'] = str(month) + ' ' + str(year)

                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'status_u_end'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'status_p_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'status_l_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'status_t_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'status_v_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'status_ts_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'status_other_start'] = str(month) + ' ' + str(year)

                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'status_v_end'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'status_p_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'status_l_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'status_t_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'status_u_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'status_ts_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'status_other_start'] = str(month) + ' ' + str(year)

                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'status_ts_end'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'status_p_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'status_l_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'status_t_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'status_u_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'status_v_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'status_other_start'] = str(month) + ' ' + str(year)

                        elif old_status == "(OT) Other":
                            master_df.at[index, 'status_other_end'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'status_p_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'status_l_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'status_t_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'status_u_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'status_v_start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'status_ts_start'] = str(month) + ' ' + str(year)

                        master_df.at[index, 'status'] = cur_status

                elif cur_sheet_type == 'Retired':
                    # changed from Planned to Retired
                    if old_status != cur_status:
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'status_p_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'status_l_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'status_t_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'status_u_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'status_v_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'status_ts_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(OT) Other":
                            master_df.at[index, 'status_other_end'] = str(month) + ' ' + str(year)

                    master_df.at[index, 'retirement_month'] = row.retirement_month
                    master_df.at[index, 'retirement_year'] = row.retirement_year
                    master_df.at[index, 'operating_month'] = row.operating_month
                    master_df.at[index, 'operating_year'] = row.operating_year
                    master_df.at[index, 'status'] = 'Retired'

                elif cur_sheet_type == 'Canceled or Postponed':
                    # change from Planned to Canceled or Postponed
                    if old_status != cur_status:
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'status_p_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'status_l_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'status_t_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'status_u_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'status_v_end'] = str(month) + ' ' + str(year)
                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'status_ts_end'] = str(month) + ' ' + str(year)
                        elif old_status == '(OT) Other':
                            master_df.at[index, 'status_other_end'] = str(month) + ' ' + str(year)

                    master_df.at[index, 'status'] = 'Canceled or Postponed'

                master_df = master_df.fillna('')

            else:
                # new project add to the result data frame
                new_row = {
                    'entity_id': row.entity_id,
                    'entity_name': row.entity_name,
                    'unit_code': row.unit_code,
                    'sector': row.sector,
                    'plant_state': row.plant_state,
                    'nameplate_capacity_mw': row.nameplate_capacity_mw,
                    'net_summer_capacity_mw': row.net_summer_capacity_mw,
                    'net_winter_capacity_mw': row.net_winter_capacity_mw,
                    'technology': row.technology,
                    'energy_source_code': row.energy_source_code,
                    'prime_mover_code': row.prime_mover_code,
                    'county': row.county,
                    'latitude': row.latitude,
                    'longitude': row.longitude,
                    'balancing_authority_code': row.balancing_authority_code,
                    'initial_report_date': str(month) + ' ' + str(year)
                }

                if cur_sheet_type == 'Operating':
                    new_row['operating_month'] = row.operating_month
                    new_row['operating_year'] = row.operating_year
                    new_row['planned_retirement_month'] = row.planned_retirement_month
                    new_row['planned_retirement_year'] = row.planned_retirement_year
                    new_row['status'] = cur_status
                    new_row['initial_report_status'] = cur_status


                elif cur_sheet_type == 'Planned':
                    new_row['og_planned_operation_month'] = row.planned_operation_month
                    new_row['cur_planned_operation_month'] = row.planned_operation_month
                    new_row['og_planned_operation_year'] = row.planned_operation_year
                    new_row['cur_planned_operation_year'] = row.planned_operation_year
                    new_row['planned_operation_delta_months'] = 0
                    new_row['status'] = cur_status
                    new_row['initial_report_status'] = cur_status

                elif cur_sheet_type == 'Retired':
                    new_row['retirement_month'] = row.retirement_month
                    new_row['retirement_year'] = row.retirement_year
                    new_row['operating_month'] = row.operating_month
                    new_row['operating_year'] = row.operating_year
                    new_row['status'] = 'Retired'
                    new_row['initial_report_status'] = 'Retired'

                elif cur_sheet_type == 'Canceled or Postponed':
                    new_row['status'] = 'Canceled or Postponed'
                    new_row['initial_report_status'] = 'Canceled or Postponed'

                new_series = pd.Series(new_row, name=index)

                master_df = master_df.append(new_series)
                master_df.fillna('', inplace=True)

        self.master_df = master_df
        master_df.to_excel('master_through_{}_{}.xlsx'.format(month, year))


def init_arg_parser():
    """
    Initialize command line argument parser
    """

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "--start",
        type=lambda s: datetime.datetime.strptime(s, '%m-%Y'),
        help="Month and year to start crawl in the format \'mm-yyyy\'",
        default=datetime.datetime(2015, 7, 1),
        required=False
    )

    arg_parser.add_argument(
        "--end",
        type=lambda s: datetime.datetime.strptime(s, '%m-%Y'),
        help="Month and year to end crawl in the format \'mm-yyyy\'",
        default=datetime.datetime(2015, 8, 1),
        required=False
    )

    return arg_parser

def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p"
    )

    crawler = Form860MCrawler(args)
    logging.info(
        'Processing dates from %d/%d through %d/%d',
        crawler.start_month,
        crawler.start_year,
        crawler.end_month,
        crawler.end_year
    )

    for month, year in crawler.crawl_range:
        filing_df = crawler.crawl_filing(month, year)
        logging.info('Finished crawling %s %d', month, year)
        crawler.update_master(filing_df, month, year)
        logging.info('Finished updating %s %d', month, year)

    crawler.master_df.to_excel('master_dataframe.xlsx')

if __name__ == "__main__":
    parser = init_arg_parser()
    args = parser.parse_args()

    main(args)
