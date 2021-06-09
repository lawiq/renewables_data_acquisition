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
        while month_iter <= self.end_month or year_iter <= self.end_year:
            crawl_range.append((calendar.month_name[month_iter], year_iter))
            if month_iter == 12:
                month_iter = 1
                year_iter += 1
            else:
                month_iter += 1

        self.crawl_range = crawl_range

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
                # change column name to "Sector" from "Sector Name"
                temp_df.rename(columns=lambda x: "Sector" if x == "Sector Name" else x, inplace=True)
                # strip the space out of column names
                temp_df.rename(columns=lambda x: x.strip(), inplace=True)
                temp_df['sheet_type'] = sheet
                sheet_dfs.append(temp_df)

            full_df = pd.concat(sheet_dfs, axis=0, ignore_index=True)
            full_df['unique_id'] = full_df.apply(
                lambda row: str(int(row['Plant ID'])) + '_' + str(row['Plant Name']) + '_' + str(row['Generator ID']),
                axis=1
            )
            full_df.set_index('unique_id', inplace=True)

            # Check that dataframe contains all needed columns, if not, add empty columns
            if 'Unit Code' not in full_df.columns:
                full_df['Unit Code'] = ""
            if 'Nameplate Capacity (MW)' not in full_df.columns:
                full_df['Nameplate Capacity (MW)'] = ""
            if 'Net Winter Capacity (MW)' not in full_df.columns:
                full_df['Net Winter Capacity (MW)'] = ""
            if 'County' not in full_df.columns:
                full_df['County'] = ""
            if 'Latitude' not in full_df.columns:
                full_df['Latitude'] = ""
            if 'Longitude' not in full_df.columns:
                full_df['Longitude'] = ""
            if 'Balancing Authority Code' not in full_df.columns:
                full_df['Balancing Authority Code'] = ""

            full_df[['Planned Operation Month']] = full_df[['Planned Operation Month']].fillna('')
            full_df[['Planned Operation Year']] = full_df[['Planned Operation Year']].fillna('')


        else:
            raise

        return full_df

    def update_master(self, filing_df, month, year):
        master_df = self.master_df
        for row in filing_df.itertuples():
            index = row[0]
            cur_status = row.Status

            if index in master_df.index:
                old_status = master_df.at[index, 'Status']

                master_df.at[index, 'Entity ID'] = row.Entity ID
                master_df.at[index, 'Entity Name'] = row['Entity Name']
                master_df.at[index, 'Sector'] = row['Sector']
                master_df.at[index, 'Unit Code'] = row['Unit Code']
                master_df.at[index, 'Plant State'] = row['Plant State']
                master_df.at[index, 'Nameplate Capacity (MW)'] = row['Nameplate Capacity (MW)']
                master_df.at[index, 'Net Summer Capacity (MW)'] = row['Net Summer Capacity (MW)']
                master_df.at[index, 'Net Winter Capacity (MW)'] = row['Net Winter Capacity (MW)']
                master_df.at[index, 'Technology'] = row['Technology']
                master_df.at[index, 'Energy Source Code'] = row['Energy Source Code']
                master_df.at[index, 'Prime Mover Code'] = row['Prime Mover Code']
                master_df.at[index, 'County'] = row['County']
                master_df.at[index, 'Latitude'] = row['Latitude']
                master_df.at[index, 'Longitude'] = row['Longitude']
                master_df.at[index, 'Balancing Authority Code'] = row['Balancing Authority Code']

                # Genrators are in service
                if row['sheet_type'] == 'Operating':
                    master_df.at[index, 'Operating Month'] = row['Operating Month']
                    master_df.at[index, 'Operating Year'] = row['Operating Year']
                    master_df.at[index, 'Planned Retirement Month'] = row['Planned Retirement Month']
                    master_df.at[index, 'Planned Retirement Year'] = row['Planned Retirement Year']

                    if old_status != cur_status:
                        # change status
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'Status P End'] = str(month) + ' ' + str(year)
                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'Status L End'] = str(month) + ' ' + str(year)
                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'Status T End'] = str(month) + ' ' + str(year)
                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'Status U End'] = str(month) + ' ' + str(year)
                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'Status V End'] = str(month) + ' ' + str(year)
                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'Status TS End'] = str(month) + ' ' + str(year)
                        elif old_status == '(OT) Other':
                            master_df.at[index, 'Status Other End'] = str(month) + ' ' + str(year)
                        # other status changes will be statuses from the Opertaing, Retired, and Canceled or Postponed sheet => retired, canceled or postponed, (SB) Standby/Backup: available for service but not normally used, (OS) Out of service and NOT expected to return to service in next calendar year, (OA) Out of service but expected to return to service in next calendar year

                        master_df.at[index, 'Status'] = cur_status


                elif row['sheet_type'] == 'Planned':
                    if str(row['Planned Operation Month']).strip() and str(row['Planned Operation Year']).strip():
                        master_df.at[index, 'Cur Planned Operation Month'] = row['Planned Operation Month']
                        master_df.at[index, 'Cur Planned Operation Year'] = row['Planned Operation Year']

                        og_planned_operation_month = master_df.at[index, 'OG Planned Operation Month']
                        og_planned_operation_year = master_df.at[index, 'OG Planned Operation Year']

                        #calculate the difference in months
                        if str(og_planned_operation_month).strip() and str(og_planned_operation_year).strip():
                            end_date = datetime.datetime(int(row['Planned Operation Year']), int(row['Planned Operation Month']), 1)
                            start_date = datetime.datetime(int(og_planned_operation_year), int(og_planned_operation_month), 1)
                            diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                            master_df.at[index, 'Planned Operation Delta (months)'] = diff
                        # there is no original planned dates when it first reported, so adding to it
                        else:
                            master_df.at[index, 'OG Planned Operation Year'] = row['Planned Operation Year']
                            master_df.at[index, 'OG Planned Operation Month'] = row['Planned Operation Month'] 

                    # change of status
                    if old_status != row['Status']:
                        # if previous status is in '(P) Planned for installation, but regulatory approvals not initiated', then change of status means phase P has ended
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'Status P End'] = str(month) + ' ' + str(year)
                            # if new status is in '(L) Regulatory approvals pending. Not under construction', then that means phase L has started
                            if cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'Status Other Start'] = str(month) + ' ' + str(year)

                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'Status L End'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'Status Other Start'] = str(month) + ' ' + str(year)

                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'Status T End'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'Status Other Start'] = str(month) + ' ' + str(year)

                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'Status U End'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'Status Other Start'] = str(month) + ' ' + str(year)

                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'Status V End'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'Status Other Start'] = str(month) + ' ' + str(year)

                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'Status TS End'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(OT) Other":
                                master_df.at[index, 'Status Other Start'] = str(month) + ' ' + str(year)

                        elif old_status == "(OT) Other":
                            master_df.at[index, 'Status Other End'] = str(month) + ' ' + str(year)
                            if cur_status == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)
                            elif cur_status == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)

                        master_df.at[index, 'Status'] = cur_status

                elif row['sheet_type'] == 'Retired':
                    # changed from Planned to Retired
                    if old_status != cur_status:
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'Status P End'] = str(month) + ' ' + str(year)
                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'Status L End'] = str(month) + ' ' + str(year)
                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'Status T End'] = str(month) + ' ' + str(year)
                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'Status U End'] = str(month) + ' ' + str(year)
                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'Status V End'] = str(month) + ' ' + str(year)
                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'Status TS End'] = str(month) + ' ' + str(year)
                        elif old_status == "(OT) Other":
                            master_df.at[index, 'Status Other End'] = str(month) + ' ' + str(year)

                    master_df.at[index, 'Retirement Month'] = row['Retirement Month']
                    master_df.at[index, 'Retirement Year'] = row['Retirement Year']
                    master_df.at[index, 'Operating Month'] = row['Operating Month']
                    master_df.at[index, 'Operating Year'] = row['Operating Year']
                    master_df.at[index, 'Status'] = 'Retired'

                elif row['sheet_type'] == 'Canceled or Postponed':
                    # change from Planned to Canceled or Postponed
                    if old_status != cur_status:
                        if old_status == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'Status P End'] = str(month) + ' ' + str(year)
                        elif old_status == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'Status L End'] = str(month) + ' ' + str(year)
                        elif old_status == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'Status T End'] = str(month) + ' ' + str(year)
                        elif old_status == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'Status U End'] = str(month) + ' ' + str(year)
                        elif old_status == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'Status V End'] = str(month) + ' ' + str(year)
                        elif old_status == "(TS) Construction complete, but not yet in commercial operation":
                            master_df.at[index, 'Status TS End'] = str(month) + ' ' + str(year)
                        elif old_status == '(OT) Other':
                            master_df.at[index, 'Status Other End'] = str(month) + ' ' + str(year)

                    master_df.at[index, 'Status'] = 'Canceled or Postponed'

                master_df = master_df.fillna('')

            else:
                # new project add to the result data frame
                new_row = {
                    'Entity ID': row['Entity ID'],
                    'Entity Name': row['Entity Name'],
                    'Unit Code': row['Unit Code'],
                    'Sector': row['Sector'],
                    'Plant State': row['Plant State'],
                    'Nameplate Capacity (MW)': row['Nameplate Capacity (MW)'],
                    'Net Summer Capacity (MW)': row['Net Summer Capacity (MW)'],
                    'Net Winter Capacity (MW)': row['Net Winter Capacity (MW)'],
                    'Technology': row['Technology'],
                    'Energy Source Code': row['Energy Source Code'],
                    'Prime Mover Code': row['Prime Mover Code'],
                    'County': row['County'],
                    'Latitude': row['Latitude'],
                    'Longitude': row['Longitude'],
                    'Balancing Authority Code': row['Balancing Authority Code'],
                    'Initial Report Date': str(month) + ' ' + str(year)
                }

                if row['sheet_type'] == 'Operating':
                    new_row['Operating Month'] = row['Operating Month']
                    new_row['Operating Year'] = row['Operating Year']
                    new_row['Planned Retirement Month'] = row['Planned Retirement Month']
                    new_row['Planned Retirement Year'] = row['Planned Retirement Year']
                    new_row['Status'] = cur_status
                    new_row['Initial Report Status'] = cur_status


                elif row['sheet_type'] == 'Planned':
                    new_row['OG Planned Operation Month'] = row['Planned Operation Month']
                    new_row['Cur Planned Operation Month'] = row['Planned Operation Month']
                    new_row['OG Planned Operation Year'] = row['Planned Operation Year']
                    new_row['Cur Planned Operation Year'] = row['Planned Operation Year']
                    new_row['Planned Operation Delta (months)'] = 0
                    new_row['Status'] = cur_status
                    new_row['Initial Report Status'] = cur_status

                elif row['sheet_type'] == 'Retired':
                    new_row['Retirement Month'] = row['Retirement Month']
                    new_row['Retirement Year'] = row['Retirement Year']
                    new_row['Operating Month'] = row['Operating Month']
                    new_row['Operating Year'] = row['Operating Year']
                    new_row['Status'] = 'Retired'
                    new_row['Initial Report Status'] = 'Retired'

                elif row['sheet_type'] == 'Canceled or Postponed':
                    new_row['Status'] = 'Canceled or Postponed'
                    new_row['Initial Report Status'] = 'Canceled or Postponed'

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
