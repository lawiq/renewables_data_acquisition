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

        self.end_month = datetime.datetime.today().date().month - 3
        self.end_year = datetime.datetime.today().date().year

        month_iter, year_iter = self.start_month, self.start_year

        crawl_range = []
        while month_iter != self.end_month or year_iter != self.end_year:
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

            skip_row_num = settings.SKIP_ROW_MAPPING[year]

            sheet_dfs = []
            for sheet in settings.SHEETS:
                temp_df = excel_file.parse(sheet, skiprows=skip_row_num)
                temp_df.drop(temp_df.tail(1).index, inplace=True)
                temp_df.rename(columns=lambda x: x.strip(), inplace=True)
                temp_df['sheet_type'] = sheet
                sheet_dfs.append(temp_df)

            full_df = pd.concat(sheet_dfs)
            full_df['unique_id'] = full_df.apply(
                lambda row: str(int(row['Plant ID'])) + '_' + str(row['Plant Name']) + '_' + str(row['Generator ID']),
                axis=1
            )
            full_df.set_index('unique_id', inplace=True)

            # TO-DO: Check that dataframe contains all needed columns, if not, add empty columns

        else:
            raise

        return full_df

    def update_master(self, filing_df, month, year):
        master_df = self.master_df
        for index, row in filing_df.iterrows():
            if index in master_df.index:
                # TO-DO: Fetch and assign variables such as 'Status' once, for later reference
                #        Construct month + year value ahead of time for later reference

                master_df.at[index, 'Entity ID'] = row['Entity ID']
                master_df.at[index, 'Entity Name'] = row['Entity Name']
                # master_df.at[index, 'Sector'] = row['Sector']
                # master_df.at[index, 'Unit Code'] = row['Unit Code']
                master_df.at[index, 'Plant State'] = row['Plant State']
                # master_df.at[index, 'Nameplate Capacity (MW)'] = row['Nameplate Capacity (MW)']
                master_df.at[index, 'Net Summer Capacity (MW)'] = row['Net Summer Capacity (MW)']
                # master_df.at[index, 'Net Winter Capacity (MW)'] = row['Net Winter Capacity (MW)']
                master_df.at[index, 'Technology'] = row['Technology']
                master_df.at[index, 'Energy Source Code'] = row['Energy Source Code']
                master_df.at[index, 'Prime Mover Code'] = row['Prime Mover Code']
                # master_df.at[index, 'County'] = row['County']
                # master_df.at[index, 'Latitude'] = row['Latitude']
                # master_df.at[index, 'Longitude'] = row['Longitude']
                # master_df.at[index, 'Balancing Authority Code'] = row['Balancing Authority Code']

                if row['sheet_type'] == 'Operating':
                    master_df.at[index, 'Operating Month'] = row['Operating Month']
                    master_df.at[index, 'Operating Year'] = row['Operating Year']
                    master_df.at[index, 'Planned Retirement Month'] = row['Planned Retirement Month']
                    master_df.at[index, 'Planned Retirement Year'] = row['Planned Retirement Year']
                    if master_df.at[index, 'Status'] != row['Status']:

                        # # change status from 'Planned' statuses
                        if master_df.at[index, 'Status'] in ['(P) Planned for installation, but regulatory approvals not initiated', '(L) Regulatory approvals pending. Not under construction', '(T) Regulatory approvals received. Not under construction', '(U) Under construction, less than or equal to 50 percent complete', '(V) Under construction, more than 50 percent complete', '(TS) Construction complete, but not yet in commercial operation', '(OT) Other']:
                            if master_df.at[index, 'Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P End'] = str(month) + ' ' + str(year)

                            elif master_df.at[index, 'Status'] == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L End'] = str(month) + ' ' + str(year)

                            elif master_df.at[index, 'Status'] == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T End'] = str(month) + ' ' + str(year)

                            elif master_df.at[index, 'Status'] == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U End'] = str(month) + ' ' + str(year)

                            elif master_df.at[index, 'Status'] == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V End'] = str(month) + ' ' + str(year)

                            elif master_df.at[index, 'Status'] == "(TS) Construction complete, but not yet in commercial":
                                master_df.at[index, 'Status TS End'] = str(month) + ' ' + str(year)

                        # change status from 'Operating' or other statuses
                        master_df.at[index, 'Status'] = row['Status']


                elif row['sheet_type'] == 'Planned':
                    if str(row['Planned Operation Month']).strip() and str(row['Planned Operation Year']).strip():
                        master_df.at[index, 'Cur Planned Operation Month'] = row['Planned Operation Month']

                        og_planned_operation_month = master_df.at[index, 'OG Planned Operation Month']

                        master_df.at[index, 'Cur Planned Operation Year'] = row['Planned Operation Year']
                        og_planned_operation_year = master_df.at[index, 'OG Planned Operation Year']

                        #calculate the difference in months
                        if str(og_planned_operation_month).strip() and str(og_planned_operation_year).strip():
                            end_date = datetime.datetime(int(row['Planned Operation Year']), int(row['Planned Operation Month']), 1)
                            start_date = datetime.datetime(int(og_planned_operation_year), int(og_planned_operation_month), 1)
                            diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                            master_df.at[index, 'Planned Operation Delta (months)'] = diff

                    # change of status
                    if master_df.at[index, 'Status'] != row['Status']:
                        # if previous status is in '(P) Planned for installation, but regulatory approvals not initiated', then change of status means phase P has ended
                        if master_df.at[index, 'Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
                            master_df.at[index, 'Status P End'] = str(month) + ' ' + str(year)
                            # if new status is in '(L) Regulatory approvals pending. Not under construction', then that means phase L has started
                            if row['Status'] == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)


                        elif master_df.at[index, 'Status'] == "(L) Regulatory approvals pending. Not under construction":
                            master_df.at[index, 'Status L End'] = str(month) + ' ' + str(year)
                            if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(T) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)

                        elif master_df.at[index, 'Status'] == "(T) Regulatory approvals received. Not under construction":
                            master_df.at[index, 'Status T End'] = str(month) + ' ' + str(year)
                            if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)

                        elif master_df.at[index, 'Status'] == "(U) Under construction, less than or equal to 50 percent complete":
                            master_df.at[index, 'Status U End'] = str(month) + ' ' + str(year)
                            if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)


                        elif master_df.at[index, 'Status'] == "(V) Under construction, more than 50 percent complete":
                            master_df.at[index, 'Status V End'] = str(month) + ' ' + str(year)
                            if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
                                master_df.at[index, 'Status TS Start'] = str(month) + ' ' + str(year)


                        elif master_df.at[index, 'Status'] == "(TS) Construction complete, but not yet in commercial":
                            master_df.at[index, 'Status TS End'] = str(month) + ' ' + str(year)
                            if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
                                master_df.at[index, 'Status P Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
                                master_df.at[index, 'Status L Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
                                master_df.at[index, 'Status T Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
                                master_df.at[index, 'Status U Start'] = str(month) + ' ' + str(year)

                            elif row['Status'] == "(V) Under construction, more than 50 percent complete":
                                master_df.at[index, 'Status V Start'] = str(month) + ' ' + str(year)

                        master_df.at[index, 'Status'] = row['Status']

                elif row['sheet_type'] == 'Retired':
                    master_df.at[index,'Retirement Month'] = row['Retirement Month']
                    master_df.at[index,'Retirement Year'] = row['Retirement Year']
                    master_df.at[index,'Operating Month'] = row['Operating Month']
                    master_df.at[index,'Operating Year'] = row['Operating Year']
                    master_df.at[index, 'Status'] = 'Retired'

                elif row['sheet_type'] == 'Canceled or Postponed':
                    master_df.at[index, 'Status'] = 'Canceled or Postponed'

                master_df = master_df.fillna('')

            else:
                # new project add to the result data frame
                new_row = {
                    'Entity ID': row['Entity ID'], 
                    'Entity Name': row['Entity Name'], 
                    # 'Unit Code': row['Unit Code'],
                    # 'Sector': row['Sector'], 
                    'Plant State': row['Plant State'], 
                    # 'Nameplate Capacity (MW)': row['Nameplate Capacity (MW)'],
                    'Net Summer Capacity (MW)': row['Net Summer Capacity (MW)'],
                    # 'Net Winter Capacity (MW)': row['Net Winter Capacity (MW)'], 
                    'Technology': row['Technology'],
                    'Energy Source Code': row['Energy Source Code'], 
                    'Prime Mover Code': row['Prime Mover Code'], 
                    # 'County': row['County'], 
                    # 'Latitude': row['Latitude'], 
                    # 'Longitude': row['Longitude'], 
                    # 'Balancing Authority Code': row['Balancing Authority Code']
                }

                if row['sheet_type'] == 'Operating':
                    new_row['Operating Month'] = row['Operating Month']
                    new_row['Operating Year'] = row['Operating Year']
                    new_row['Planned Retirement Month'] = row['Planned Retirement Month']
                    new_row['Planned Retirement Year'] = row['Planned Retirement Year']
                    new_row['Status'] = row['Status']


                elif row['sheet_type'] == 'Planned':
                    new_row['OG Planned Operation Month'] = row['Planned Operation Month']
                    new_row['Cur Planned Operation Month'] = row['Planned Operation Month']

                    new_row['OG Planned Operation Year'] = row['Planned Operation Year']
                    new_row['Cur Planned Operation Year'] = row['Planned Operation Year']

                    new_row['Planned Operation Delta (months)'] = 0

                    new_row['Status'] = row['Status']

                elif row['sheet_type'] == 'Retired':
                    new_row['Retirement Month'] = row['Retirement Month']
                    new_row['Retirement Year'] = row['Retirement Year']
                    new_row['Operating Month'] = row['Operating Month']
                    new_row['Operating Year'] = row['Operating Year']
                    new_row['Status'] = 'Retired'

                elif row['sheet_type'] == 'Canceled or Postponed':
                    new_row['Status'] = 'Canceled or Postponed'

                new_series = pd.Series(new_row, name=index)

                master_df = master_df.append(new_series)
                master_df.fillna('', inplace=True)

        self.master_df = master_df

def init_arg_parser():
    """
    Initialize command line argument parser
    """

    arg_parser = argparse.ArgumentParser()

    # TO-DO: Add argument for specifying end date, with default as current
    #        Add argument for specifying existing spreadsheet to use as 'master_df'
    #        Add argument for writing 'master_df' to csv after every successful update

    arg_parser.add_argument(
        "--start",
        type=lambda s: datetime.datetime.strptime(s, '%m-%Y'),
        help="Month and year to start crawl in the format \'mm-yyyy\'",
        default=datetime.datetime(2015, 7, 1),
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

    self.master_df.to_csv('master_dataframe.csv')

if __name__ == "__main__":
    parser = init_arg_parser()
    args = parser.parse_args()

    main(args)