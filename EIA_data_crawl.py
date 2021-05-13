import pandas as pd
import calendar
import requests

EIA_API_KEY = '8f7af15657106a9e3101178ff1d9999c'

url = "https://www.eia.gov/electricity/data/eia860m/archive/xls/{}_generator2020.xlsx"

months = calendar.month_name[11:13]
fields = ['Entity ID', 'Entity Name', 'Name', 'Sector', 'Plant State', 'Nameplate Capacity (MW)', 'Net Summer Capacity (MW)', 'Net Winter Capacity (MW)', 'Technology', 'Energy Source Code', 'Prime Mover Code', 'Planned Operation Month', 'Planned Operation Year', 'Opertating Month', 'Operating Year', 'Planned Retirement Month', 'Planned Retirement Year', 'Retirement Month', 'Retirement Year', 'Status', 'Status P Start', 'Status P End', 'Status L Start', 'Status L End', 'Status T Start', 'Status T End', 'Status U Start', 'Status U End', 'Status V Start', 'Status V End', 'Status TS Start', 'Status TS End', 'County', 'Latitude', 'Longitude', 'Balancing Authority Code']


def process_dataframe(aggregated_df, sheet_type, current_df, month):
	print("processing {}".format(month))
	for index, row in current_df.iterrows():
		try:
			unit_code = '' if pd.isna(row['Unit Code']) else '_{}'.format(str(row['Unit Code']))
			name = str(int(row['Plant ID'])) + '_' + str(row['Plant Name']) + '_' + str(row['Generator ID']) + unit_code

			# check if project already exist, the update the project
			results = aggregated_df.isin({'Name': [name]})
			if results['Name'].any():
				print(index)
				# get the in dex of exsiting project
				ele_index = results['Name'][results['Name'] == True].index[0]

				aggregated_df.at[ele_index, 'Entity ID'] = row['Entity ID']
				aggregated_df.at[ele_index, 'Entity Name'] = row['Entity Name']
				aggregated_df.at[ele_index, 'Sector'] = row['Sector']
				aggregated_df.at[ele_index, 'Plant State'] = row['Plant State']
				aggregated_df.at[ele_index, 'Nameplate Capacity (MW)'] = row['Nameplate Capacity (MW)']
				aggregated_df.at[ele_index, 'Net Summer Capacity (MW)'] = row['Net Summer Capacity (MW)']
				aggregated_df.at[ele_index, 'Net Winter Capacity (MW)'] = row['Net Winter Capacity (MW)']
				aggregated_df.at[ele_index, 'Technology'] = row['Technology']
				aggregated_df.at[ele_index, 'Energy Source Code'] = row['Energy Source Code']
				aggregated_df.at[ele_index, 'Prime Mover Code'] = row['Prime Mover Code']
				aggregated_df.at[ele_index, 'County'] = row['County']
				aggregated_df.at[ele_index, 'Latitude'] = row['Latitude']
				aggregated_df.at[ele_index, 'Longitude'] = row['Longitude']
				aggregated_df.at[ele_index, 'Balancing Authority Code'] = row['Balancing Authority Code']

				if sheet_type == 'Operating':
					aggregated_df.at[ele_index, 'Opertating Month'] = row['Operating Month']
					aggregated_df.at[ele_index, 'Operating Year'] = row['Operating Year']
					aggregated_df.at[ele_index, 'Planned Retirement Month'] = row['Planned Retirement Month']
					aggregated_df.at[ele_index, 'Planned Retirement Year'] = row['Planned Retirement Year']
					if aggregated_df.at[ele_index, 'Status'] != row['Status']:

						# # change status from 'Planned' statuses
						if aggregated_df.at[ele_index, 'Status'] in ['(P) Planned for installation, but regulatory approvals not initiated', '(L) Regulatory approvals pending. Not under construction', '(T) Regulatory approvals received. Not under construction', '(U) Under construction, less than or equal to 50 percent complete', '(V) Under construction, more than 50 percent complete', '(TS) Construction complete, but not yet in commercial operation', '(OT) Other']:
							if aggregated_df.at[ele_index, 'Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
								aggregated_df.at[ele_index, 'Status P End'] = month

							elif aggregated_df.at[ele_index, 'Status'] == "(L) Regulatory approvals pending. Not under construction":
								aggregated_df.at[ele_index, 'Status L End'] = month

							elif aggregated_df.at[ele_index, 'Status'] == "(T) Regulatory approvals received. Not under construction":
								aggregated_df.at[ele_index, 'Status T End'] = month

							elif aggregated_df.at[ele_index, 'Status'] == "(U) Under construction, less than or equal to 50 percent complete":
								aggregated_df.at[ele_index, 'Status U End'] = month

							elif aggregated_df.at[ele_index, 'Status'] == "(V) Under construction, more than 50 percent complete":
								aggregated_df.at[ele_index, 'Status V End'] = month

							elif aggregated_df.at[ele_index, 'Status'] == "(TS) Construction complete, but not yet in commercial":
								aggregated_df.at[ele_index, 'Status TS End'] = month

						# change status from 'Operating' or other statuses
						aggregated_df.at[ele_index, 'Status'] = row['Status']


				elif sheet_type == 'Planned':
					aggregated_df.at[ele_index, 'Planned Operation Month'] = row['Planned Operation Month']
					aggregated_df.at[ele_index, 'Planned Operation Year'] = row['Planned Operation Year']
					# change of status
					if aggregated_df.at[ele_index, 'Status'] != row['Status']:
						# if previous status is in '(P) Planned for installation, but regulatory approvals not initiated', then change of status means phase P has ended
						if aggregated_df.at[ele_index, 'Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
							aggregated_df.at[ele_index, 'Status P End'] = month
							# if new status is in '(L) Regulatory approvals pending. Not under construction', then that means phase L has started
							if row['Status'] == "(L) Regulatory approvals pending. Not under construction":
								aggregated_df.at[ele_index, 'Status L Start'] = month

							elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
								aggregated_df.at[ele_index, 'Status T Start'] = month

							elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
								aggregated_df.at[ele_index, 'Status U Start'] = month

							elif row['Status'] == "(V) Under construction, more than 50 percent complete":
								aggregated_df.at[ele_index, 'Status V Start'] = month

							elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
								aggregated_df.at[ele_index, 'Status TS Start'] = month


						elif aggregated_df.at[ele_index, 'Status'] == "(L) Regulatory approvals pending. Not under construction":
							aggregated_df.at[ele_index, 'Status L End'] = month
							if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
								aggregated_df.at[ele_index, 'Status P Start'] = month

							elif row['Status'] == "(T) Under construction, less than or equal to 50 percent complete":
								aggregated_df.at[ele_index, 'Status T Start'] = month

							elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
								aggregated_df.at[ele_index, 'Status U Start'] = month

							elif row['Status'] == "(V) Under construction, more than 50 percent complete":
								aggregated_df.at[ele_index, 'Status V Start'] = month

							elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
								aggregated_df.at[ele_index, 'Status TS Start'] = month

						elif aggregated_df.at[ele_index, 'Status'] == "(T) Regulatory approvals received. Not under construction":
							aggregated_df.at[ele_index, 'Status T End'] = month
							if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
								aggregated_df.at[ele_index, 'Status P Start'] = month

							elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
								aggregated_df.at[ele_index, 'Status L Start'] = month

							elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
								aggregated_df.at[ele_index, 'Status U Start'] = month

							elif row['Status'] == "(V) Under construction, more than 50 percent complete":
								aggregated_df.at[ele_index, 'Status V Start'] = month

							elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
								aggregated_df.at[ele_index, 'Status TS Start'] = month

						elif aggregated_df.at[ele_index, 'Status'] == "(U) Under construction, less than or equal to 50 percent complete":
							aggregated_df.at[ele_index, 'Status U End'] = month
							if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
								aggregated_df.at[ele_index, 'Status P Start'] = month

							elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
								aggregated_df.at[ele_index, 'Status L Start'] = month

							elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
								aggregated_df.at[ele_index, 'Status T Start'] = month

							elif row['Status'] == "(V) Under construction, more than 50 percent complete":
								aggregated_df.at[ele_index, 'Status V Start'] = month

							elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
								aggregated_df.at[ele_index, 'Status TS Start'] = month


						elif aggregated_df.at[ele_index, 'Status'] == "(V) Under construction, more than 50 percent complete":
							aggregated_df.at[ele_index, 'Status V End'] = month
							if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
								aggregated_df.at[ele_index, 'Status P Start'] = month

							elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
								aggregated_df.at[ele_index, 'Status L Start'] = month

							elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
								aggregated_df.at[ele_index, 'Status T Start'] = month

							elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
								aggregated_df.at[ele_index, 'Status U Start'] = month

							elif row['Status'] == "(TS) Construction complete, but not yet in commercial operation":
								aggregated_df.at[ele_index, 'Status TS Start'] = month


						elif aggregated_df.at[ele_index, 'Status'] == "(TS) Construction complete, but not yet in commercial":
							aggregated_df.at[ele_index, 'Status TS End'] = month
							if row['Status'] == "(P) Planned for installation, but regulatory approvals not initiated":
								aggregated_df.at[ele_index, 'Status P Start'] = month

							elif row['Status'] == "(L) Regulatory approvals pending. Not under construction":
								aggregated_df.at[ele_index, 'Status L Start'] = month

							elif row['Status'] == "(T) Regulatory approvals received. Not under construction":
								aggregated_df.at[ele_index, 'Status T Start'] = month

							elif row['Status'] == "(U) Under construction, less than or equal to 50 percent complete":
								aggregated_df.at[ele_index, 'Status U Start'] = month

							elif row['Status'] == "(V) Under construction, more than 50 percent complete":
								aggregated_df.at[ele_index, 'Status V Start'] = month

						aggregated_df.at[ele_index, 'Status'] = row['Status']

				elif sheet_type == 'Retired':
					aggregated_df.at[ele_index,'Retirement Month'] = row['Retirement Month']
					aggregated_df.at[ele_index,'Retirement Year'] = row['Retirement Year']
					aggregated_df.at[ele_index,'Operating Month'] = row['Operating Month']
					aggregated_df.at[ele_index,'Operating Year'] = row['Operating Year']
					aggregated_df.at[ele_index, 'Status'] = 'Retired'

				elif sheet_type == 'Canceled or Postponed':
					aggregated_df.at[ele_index, 'Status'] = 'Canceled or Postponed'

				aggregated_df.fillna('', inplace=True)

			else:
				# new project add to the result data frame
				new_row = {
					'Entity ID': row['Entity ID'], 
					'Entity Name': row['Entity Name'], 
					'Name': name,
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
					'Balancing Authority Code': row['Balancing Authority Code']
				}

				if sheet_type == 'Operating':
					new_row['Opertating Month'] = row['Operating Month']
					new_row['Operating Year'] = row['Operating Year']
					new_row['Planned Retirement Month'] = row['Planned Retirement Month']
					new_row['Planned Retirement Year'] = row['Planned Retirement Year']
					new_row['Status'] = row['Status']


				elif sheet_type == 'Planned':
					new_row['Planned Operation Month'] = row['Planned Operation Month']
					new_row['Planned Operation Year'] = row['Planned Operation Year']
					new_row['Status'] = row['Status']

				elif sheet_type == 'Retired':
					new_row['Retirement Month'] = row['Retirement Month']
					new_row['Retirement Year'] = row['Retirement Year']
					new_row['Operating Month'] = row['Operating Month']
					new_row['Operating Year'] = row['Operating Year']
					new_row['Status'] = 'Retired'

				elif sheet_type == 'Canceled or Postponed':
					new_row['Status'] = 'Canceled or Postponed'

				aggregated_df = aggregated_df.append(new_row, ignore_index = True)
				aggregated_df.fillna('', inplace=True)
		except Exception as e:
			print(e)
			print(row)
	return aggregated_df

final_file_name = 'projects.xlsx'
xl = pd.ExcelFile(final_file_name)
aggregated_df = xl.parse()

for month in months:
	r = requests.get(url.format(month.lower()))

	if r.status_code == 200:
		file_name = '{}_generator2020.xlsx'.format(month)
		with open(file_name, 'wb') as f:
			f.write(r.content)
		xl = pd.ExcelFile(file_name)

		# first process the Operating sheet
		operating_df = xl.parse('Operating', skiprows=2)
		operating_df.drop(operating_df.tail(1).index, inplace = True)
		aggregated_df = process_dataframe(aggregated_df, 'Operating', operating_df, month)

		# # process Planned sheet
		planned_df = xl.parse('Planned', skiprows=2)
		planned_df.drop(planned_df.tail(1).index, inplace = True)
		aggregated_df = process_dataframe(aggregated_df, 'Planned', planned_df, month)

		# # process Retired sheet
		retired_df = xl.parse('Retired', skiprows=2)
		retired_df.drop(retired_df.tail(1).index, inplace = True)
		aggregated_df = process_dataframe(aggregated_df, 'Retired', retired_df, month)

		# process Canceled or Postponed sheet
		canceled_or_postponed_df = xl.parse('Canceled or Postponed', skiprows=2)
		canceled_or_postponed_df.drop(canceled_or_postponed_df.tail(1).index, inplace = True)
		aggregated_df = process_dataframe(aggregated_df, 'Canceled or Postponed', canceled_or_postponed_df, month)
	aggregated_df.to_excel("projects_{}.xlsx".format(month))
aggregated_df.to_excel("final_projects.xlsx")
