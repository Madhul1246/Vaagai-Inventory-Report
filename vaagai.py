import requests
from datetime import datetime, timedelta
import pandas as pd
import time
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders


# API Configuration
BASE_URL = "https://api.vaagaibus.in/api"
TOKEN = "MTE1YzI2YzEwOWMxMDdjOTNjMTA2YzEwMmM4OWMxMDFjOTNjMjZjNTBjMjRjMjZjOTZjMTAzYzEwNGMxMTRjMTEzYzEwN2MxMDBjMjZjMzZjMjZjMTA0Yzg5YzEwN2MxMDdjMTExYzEwM2MxMDZjOTJjMjZjNTBjMjZjNjRjMTAzYzc1YzEwM2M0MmMxMDRjMTA4YzQ5YzI2YzExN2M3MA=="
PHPSESSID = "5gbecimc48p21906k1qa7edu1g"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "insomnia/11.2.0",
    "token": TOKEN,
}
COOKIES = {"PHPSESSID": PHPSESSID}

# ZOHO EMAIL CONFIGURATION
ZOHO_SMTP_SERVER = "smtp.zoho.in"
ZOHO_SMTP_PORT = 587
ZOHO_EMAIL = "madhu.l@hopzy.in"
ZOHO_PASSWORD = "JqkGLkfkTf0n"      # Zoho App Password
MANAGER_EMAIL = "avinash.sk@hopzy.in"

# CC recipients
CC_EMAILS = ["raj.shivraj@hopzy.in", "siddarth.shetty@hopzy.in", "tejus.a@hopzy.in"]


def get_operators():
    url = f"{BASE_URL}/GetOperatorList/hopzy"
    response = requests.post(url, headers=HEADERS, cookies=COOKIES, timeout=30)
    if response.status_code != 200:
        return []
    data = response.json()
    return data.get("operatorlist", [])


def get_trips_for_operator(opid, tripdate):
    url = f"{BASE_URL}/GetAllAvailableTripsOnADay/apiagent"
    payload = {"tripdate": tripdate, "opid": opid}
    try:
        response = requests.post(url, json=payload, headers=HEADERS, cookies=COOKIES, timeout=30)
        if response.status_code != 200:
            return {"availabletrips": []}
        return response.json()
    except Exception:
        return {"availabletrips": []}


def fetch_trips_for_date_range(start_date, days=7):
    print("Fetching trips for next 7 days (ALL 67 operators tracked)...")
    operators = get_operators()
    all_trips = []
    all_operator_date_status = []  # Track EVERY operator-date combo

    date_range = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

    print(f"Processing dates: {', '.join(date_range)}")
    print(f"Total operators to check: {len(operators)}")

    for date_idx, trip_date in enumerate(date_range, 1):
        print(f"\nDay {date_idx}/{days}: {trip_date}")
        daily_trips_count = 0

        for i, operator in enumerate(operators, 1):
            opid = operator.get("code")
            opname = operator.get("name", "")
            if not opid:
                continue

            trips_data = get_trips_for_operator(opid, trip_date)
            trips_list = trips_data.get("availabletrips", [])

            # TRACK EVERY operator-date status (WITH or WITHOUT data)
            status = "WITH DATA" if trips_list else "WITHOUT DATA"
            trip_count = len(trips_list)
            all_operator_date_status.append({
                "trip_date": trip_date,
                "operator_code": opid,
                "operator_name": opname,
                "status": status,
                "trip_count": trip_count
            })

            # Add trips if available
            if trips_list:
                for trip in trips_list:
                    all_trips.append({
                        "trip_date": trip_date,
                        "operator_code": opid,
                        "operatorname": opname,
                        "routeid": trip.get("routeid", ""),
                        "tripid": trip.get("tripid", ""),
                        "srcname": trip.get("srcname", ""),
                        "dstname": trip.get("dstname", ""),
                        "depaturetime": trip.get("depaturetime", ""),
                        "availseats": trip.get("availseats", ""),
                    })
                daily_trips_count += trip_count

            if i % 10 == 0:
                print(f"Processed {i}/{len(operators)} operators for {trip_date}...")
            time.sleep(0.2)

        print(
            f"{trip_date}: {daily_trips_count} trips | "
            f"{len([x for x in all_operator_date_status if x['trip_date'] == trip_date and x['status'] == 'WITH DATA'])} WITH data | "
            f"{len([x for x in all_operator_date_status if x['trip_date'] == trip_date and x['status'] == 'WITHOUT DATA'])} WITHOUT data "
            f"(Total: {len(operators)})"
        )

    # Calculate daily stats from detailed tracking
    daily_stats_dict = {}
    for date in date_range:
        date_data = [x for x in all_operator_date_status if x['trip_date'] == date]
        daily_stats_dict[date] = {
            "with_data": len([x for x in date_data if x['status'] == 'WITH DATA']),
            "without_data": len([x for x in date_data if x['status'] == 'WITHOUT DATA']),
            "total_operators": len(date_data),
            "total_schedules": sum([x['trip_count'] for x in date_data]),
        }

    return all_trips, all_operator_date_status, daily_stats_dict


def create_excel_summary(all_trips, all_operator_date_status, daily_stats_dict, start_date, days=7):
    df = pd.DataFrame(all_trips)
    date_range = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

    # 1. ALL_Operator_Date_Status sheet - EVERY operator every date
    operator_date_df = pd.DataFrame(all_operator_date_status)
    operator_date_df = operator_date_df.sort_values(['trip_date', 'operator_code'])

    # 2. Daily stats with FULL operator tracking
    daily_stats = pd.DataFrame({
        'Trip Date': date_range,
        'With Data': [daily_stats_dict[date]['with_data'] for date in date_range],
        'Without Data': [daily_stats_dict[date]['without_data'] for date in date_range],
        'Total Operators': [daily_stats_dict[date]['total_operators'] for date in date_range],
        'Total Schedules': [daily_stats_dict[date]['total_schedules'] for date in date_range],
        'Total Routes': [
            df[df['trip_date'] == date]['routeid'].nunique() if (not df.empty and date in df['trip_date'].values) else 0
            for date in date_range
        ],
        'Success Rate': [
            round(daily_stats_dict[date]['with_data'] / daily_stats_dict[date]['total_operators'] * 100, 1)
            for date in date_range
        ]
    })

    # 3. Operator summary across 7 days (INCLUDES WITHOUT DATA operators)
    operator_summary_full = operator_date_df.groupby(['operator_code', 'operator_name']).agg({
        'trip_count': 'sum',
        'status': lambda x: 'WITH DATA' if (x == 'WITH DATA').any() else 'WITHOUT DATA'
    }).reset_index()
    operator_summary_full.columns = ['Operator Code', 'Operator Name', 'Total Trips (7 days)', 'Status (7 days)']
    operator_summary_full = operator_summary_full.sort_values('Total Trips (7 days)', ascending=False)

    # Overall stats
    total_trips = len(df)
    total_routes = df['routeid'].nunique() if not df.empty else 0
    total_operators_with_data = len(operator_summary_full[operator_summary_full['Total Trips (7 days)'] > 0])
    total_operators_checked = len(operator_summary_full)

    # NEW: Operator Inventory Matrix (rows = operator, columns = date-wise unique routes and schedules)
    if not df.empty:
        per_op_day = df.groupby(['operator_code', 'trip_date']).agg(
            Unique_Routes=('routeid', 'nunique'),
            Total_Schedules=('tripid', 'count')
        ).reset_index()
    else:
        per_op_day = pd.DataFrame(columns=['operator_code', 'trip_date', 'Unique_Routes', 'Total_Schedules'])

    inventory_matrix = pd.DataFrame({'operator_code': operator_summary_full['Operator Code']})
    for date in date_range:
        temp = per_op_day[per_op_day['trip_date'] == date][['operator_code', 'Unique_Routes', 'Total_Schedules']]
        temp = temp.copy()
        temp.columns = ['operator_code', f'{date}_Routes', f'{date}_Schedules']
        inventory_matrix = inventory_matrix.merge(temp, on='operator_code', how='left')

    inventory_matrix = inventory_matrix.fillna(0)

    inventory_matrix = inventory_matrix.merge(
        operator_summary_full[['Operator Code', 'Operator Name']],
        left_on='operator_code',
        right_on='Operator Code',
        how='left'
    )
    inventory_matrix.drop(columns=['Operator Code'], inplace=True)
    cols = ['operator_code', 'Operator Name'] + [
        c for c in inventory_matrix.columns if c not in ['operator_code', 'Operator Name']
    ]
    inventory_matrix = inventory_matrix[cols]

    # Filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_range_str = f"{start_date.strftime('%Y%m%d')}_to_{(start_date + timedelta(days=days - 1)).strftime('%Y%m%d')}"
    excel_filename = f"vaagaibus_7days_ALL_67operators_{date_range_str}_{timestamp}.xlsx"

    # Save all sheets
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        if not df.empty:
            df.to_excel(writer, sheet_name='1_All_Trips_WITH_Data', index=False)

        operator_date_df.to_excel(writer, sheet_name='2_ALL_Operator_Date_Status', index=False)
        daily_stats.to_excel(writer, sheet_name='3_Daily_Stats_Email', index=False)
        operator_summary_full.to_excel(writer, sheet_name='4_Operator_Summary_7days', index=False)

        if not df.empty:
            daily_summary = df.groupby('trip_date').agg({
                'tripid': 'count',
                'routeid': 'nunique',
                'operator_code': 'nunique'
            }).reset_index()
            daily_summary.columns = ['Trip Date', 'Total Trips', 'Unique Routes', 'Operators (WITH Data)']
            daily_summary.to_excel(writer, sheet_name='5_Daily_Summary_WITH_Data', index=False)

            top_routes = df.groupby(['routeid', 'srcname', 'dstname']).agg({
                'tripid': 'count'
            }).reset_index()
            top_routes.columns = ['Route ID', 'Source', 'Destination', 'Total Trips']
            top_routes = top_routes.sort_values('Total Trips', ascending=False)
            top_routes.to_excel(writer, sheet_name='6_Top_Routes', index=False)

        overall_stats = pd.DataFrame({
            'Metric': [
                'Total Trips (7 days)',
                'Total Unique Routes',
                'Operators WITH Data (7 days)',
                'Operators WITHOUT Data (some days)',
                'Total Operators Checked',
                'Total Days',
                'Total Operator-Date Checks'
            ],
            'Count': [
                total_trips,
                total_routes,
                total_operators_with_data,
                total_operators_checked - total_operators_with_data,
                total_operators_checked,
                days,
                len(operator_date_df)
            ]
        })
        overall_stats.to_excel(writer, sheet_name='7_Overall_Stats', index=False)

        # NEW SHEET
        inventory_matrix.to_excel(writer, sheet_name='8_Operator_Inventory_Matrix', index=False)

    print(f"Excel saved with 8 sheets - ALL 67 operators × {days} days = {len(operator_date_df)} rows:")
    print(f"   {excel_filename}")
    print("   2_ALL_Operator_Date_Status: EVERY operator every date WITH/WITHOUT data")
    print("   4_Operator_Summary_7days: 67 operators total trips + status")

    return excel_filename, operator_summary_full, daily_stats, total_trips, total_routes, total_operators_checked


def create_html_summary(total_trips, total_routes, total_operators,
                        operator_summary, daily_stats, start_date, days=7):
    end_date = start_date + timedelta(days=days - 1)

    # Build HTML rows for daily stats (shows ALL 67 operators)
    daily_rows = ""
    for _, row in daily_stats.iterrows():
        date_str = row['Trip Date']
        with_data = int(row['With Data'])
        without_data = int(row['Without Data'])
        total_ops = int(row['Total Operators'])
        total_sched = int(row['Total Schedules'])
        total_routes_row = int(row['Total Routes'])
        success_rate = float(row['Success Rate'])

        daily_rows += f"""
        <tr>
         <td style="padding:6px 8px; border:1px solid #d9d9d9;">{date_str}</td>
         <td align="right" style="padding:6px 8px; border:1px solid #d9d9d9; color:#27ae60; font-weight:bold;">{with_data}</td>
         <td align="right" style="padding:6px 8px; border:1px solid #d9d9d9; color:#e74c3c; font-weight:bold;">{without_data}</td>
         <td align="right" style="padding:6px 8px; border:1px solid #d9d9d9; font-weight:bold;">{total_ops}</td>
         <td align="right" style="padding:6px 8px; border:1px solid #d9d9d9;">{total_sched:,}</td>
         <td align="right" style="padding:6px 8px; border:1px solid #d9d9d9;">{total_routes_row}</td>
         <td align="right" style="padding:6px 8px; border:1px solid #d9d9d9;">{success_rate:.1f}%</td>
        </tr>
        """

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.5; color: #333; margin:0; padding:0; background:#f5f5f5;">
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5; padding:20px 0;">
        <tr>
          <td align="center">
            <table width="900" cellpadding="0" cellspacing="0" style="background:#ffffff; border:1px solid #d9d9d9; max-width:900px;">
              <!-- Header with logo and title -->
              <tr>
                <td align="left" style="padding:14px 20px 8px 20px; border-bottom:1px solid #e0e0e0;">
                  <table width="100%" cellpadding="0" cellspacing="0">
                    <tr>
                      <td align="left" valign="middle">
                        <img src="cid:vaagaibus_logo" alt="Vaagaibus" style="height:32px;">
                      </td>
                      <td align="right" valign="middle" style="font-size:18px; font-weight:bold; color:#0066cc;">
                        7-Day Operator Inventory Report
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <!-- Period info -->
              <tr>
                <td style="padding:10px 20px; font-size:12px; color:#555; background:#f8f9fa; border-bottom:1px solid #e0e0e0;">
                  <div><strong>Period:</strong> {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}</div>
                  <div><strong>Generated:</strong> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</div>
                  <div><strong>Total Operators Checked:</strong> {total_operators}</div>
                </td>
              </tr>

              <!-- Overall Summary -->
              <tr>
                <td style="padding:16px 20px 8px 20px;">
                  <div style="font-size:14px; font-weight:bold; color:#333; margin-bottom:6px;">Overall Summary (Next 7 Days)</div>
                  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; font-size:13px; margin-bottom:16px;">
                    <tr style="background:#0066cc; color:#ffffff;">
                      <th align="left" style="padding:8px 10px; border:1px solid #d9d9d9;">Metric</th>
                      <th align="right" style="padding:8px 10px; border:1px solid #d9d9d9;">Value</th>
                    </tr>
                    <tr>
                      <td style="padding:8px 10px; border:1px solid #d9d9d9;">Total Trips (Next 7 Days)</td>
                      <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; color:#27ae60; font-weight:bold; font-size:15px;">{total_trips:,}</td>
                    </tr>
                    <tr style="background:#f8f9fa;">
                      <td style="padding:8px 10px; border:1px solid #d9d9d9;">Total Unique Routes</td>
                      <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; color:#e67e22; font-weight:bold;">{total_routes:,}</td>
                    </tr>
                    <tr>
                      <td style="padding:8px 10px; border:1px solid #d9d9d9;">Total Operators</td>
                      <td align="right" style="padding:8px 10px; border:1px solid #d9d9d9; color:#3498db; font-weight:bold;">{total_operators}</td>
                    </tr>
                  </table>
                </td>
              </tr>

              <!-- Daily stats -->
              <tr>
                <td style="padding:0 20px 16px 20px;">
                  <div style="font-size:14px; font-weight:bold; color:#333; margin-bottom:6px;">Daily stats</div>
                  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; font-size:12px;">
                    <tr style="background:#0066cc; color:#ffffff;">
                      <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="left">Date</th>
                      <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">With Data</th>
                      <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Without Data</th>
                      <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Total Operators</th>
                      <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Total Schedules</th>
                      <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Total Routes</th>
                      <th style="padding:8px 10px; border:1px solid #d9d9d9;" align="right">Success Rate</th>
                    </tr>
                    {daily_rows}
                  </table>
                </td>
              </tr>

              <!-- Footer -->
              <tr>
                <td style="padding:14px 20px; font-size:11px; color:#777; border-top:1px solid #e0e0e0; background:#f8f9fa;">
                  <strong>Attachments:</strong> 7-day Excel report (All Trips, Daily Summary, Operator Summary, Top Routes, Overall Stats, Operator Inventory Matrix).<br>
                  <span>Automated 7-day report by Vaagaibus Data Collector.</span>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>
    """
    return html_body


def send_zoho_email(subject, html_body, excel_filename, manager_email, cc_emails=None):
    msg = MIMEMultipart('mixed')
    msg['From'] = ZOHO_EMAIL
    msg['To'] = manager_email
    if cc_emails:
        msg['Cc'] = ", ".join(cc_emails)
    msg['Subject'] = subject

    # Related part for HTML and inline images
    alternative_part = MIMEMultipart('related')
    alternative_part.attach(MIMEText(html_body, 'html'))

    # Inline logo image (image.jpg in same directory)
    try:
        with open("image.jpg", "rb") as img_file:
            logo = MIMEImage(img_file.read())
            logo.add_header('Content-ID', '<vaagaibus_logo>')
            logo.add_header('Content-Disposition', 'inline', filename="logo.jpg")
            alternative_part.attach(logo)
    except Exception as e:
        print(f"Logo not attached: {e}")

    msg.attach(alternative_part)

    # Excel attachment
    with open(excel_filename, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

    encoders.encode_base64(part)
    part.add_header(
        'Content-Disposition',
        f'attachment; filename= {os.path.basename(excel_filename)}'
    )
    msg.attach(part)

    # Recipients list including CC
    recipients = [manager_email]
    if cc_emails:
        recipients.extend(cc_emails)

    try:
        server = smtplib.SMTP(ZOHO_SMTP_SERVER, ZOHO_SMTP_PORT)
        server.starttls()
        server.login(ZOHO_EMAIL, ZOHO_PASSWORD)
        server.sendmail(ZOHO_EMAIL, recipients, msg.as_string())
        server.quit()
        print("Email sent successfully!")
        return True
    except Exception as e:
        print(f"Email failed: {e}")
        return False


def main():
    start_date = datetime.today() + timedelta(days=1)  # Start from tomorrow
    days = 7

    print("Vaagaibus 7-Day COMPLETE Report Generator (ALL 67 Operators + WITHOUT DATA)")
    print(f"Period: {start_date.strftime('%Y-%m-%d')} to {(start_date + timedelta(days=days - 1)).strftime('%Y-%m-%d')}")

    all_trips, all_operator_date_status, daily_stats_dict = fetch_trips_for_date_range(start_date, days)

    print(f"\nTotal trips collected: {len(all_trips):,}")
    print(f"Total operator-date records: {len(all_operator_date_status)} (67 × {days} days)")

    excel_filename, operator_summary, daily_stats, total_trips, total_routes, total_operators = \
        create_excel_summary(all_trips, all_operator_date_status, daily_stats_dict, start_date, days)

    if not excel_filename:
        print("No Excel file created")
        return

    html_body = create_html_summary(
        total_trips, total_routes, total_operators,
        operator_summary, daily_stats, start_date, days
    )

    end_date = start_date + timedelta(days=days - 1)
    subject = f"Vaagaibus: 7-Day Inventory Report (ALL 67 Operators) - {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}"

    success = send_zoho_email(subject, html_body, excel_filename, MANAGER_EMAIL, cc_emails=CC_EMAILS)

    if success:
        print("\nCOMPLETE 7-DAY REPORT SENT SUCCESSFULLY!")
        print(f"To: {MANAGER_EMAIL}")
        if CC_EMAILS:
            print(f"Cc: {', '.join(CC_EMAILS)}")
        print(f"Excel: {excel_filename}")
        print(f"{total_operators} operators checked | {total_routes:,} routes | {total_trips:,} trips")
        print(f"2_ALL_Operator_Date_Status sheet has ALL {total_operators}×{days} operator-date records!")
    else:
        print("\nExcel saved locally but email failed - check file manually")


if __name__ == "__main__":
    main()
