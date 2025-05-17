# -------------------------------------------------------------------------
# Durable Functions Refactor of Your Existing Code with Try/Except
# and a dedicated "process_parcels_activity"
# -------------------------------------------------------------------------
import datetime
import logging
import json
import time
import os
import warnings
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO

import xlsxwriter
import pytz
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

# Azure Function / Key Vault / Durable Imports
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Durable-specific imports
import azure.durable_functions as df
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import DurableOrchestrationClient
from azure.durable_functions import RetryOptions

# -------------------------------------------------------------------------
# Initialize the FunctionApp and Key Vault client
# -------------------------------------------------------------------------
app = df.DFApp()

key_vault_url = "https://premo-vault.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_url, credential=credential)

# -------------------------------------------------------------------------
# Utility to Decide if Script Should Run
# -------------------------------------------------------------------------
def should_run_script():
    """ Will run at 12:00 Stockholm time """
    time_now = datetime.datetime.now(pytz.timezone('Europe/Stockholm'))
    current_time = time_now.time()
    start_time = datetime.time(11, 50, 0)
    end_time = datetime.time(12, 10, 0)
    in_window = start_time <= current_time < end_time
    logging.debug(f"Current Stockholm time: {current_time}, In window: {in_window}")
    return in_window

# -------------------------------------------------------------------------
# Activity: Send Email (same as original, just wrapped in an Activity)
# -------------------------------------------------------------------------
@app.activity_trigger(input_name="args")
def send_email_with_excel_from_df(args: dict):
    """
    Activity to send an email with an optional Excel attachment derived from a Pandas DataFrame.
    """
    sender_email = args['sender_email']
    sender_password = args['sender_password']
    to_emails = args['to_emails']
    cc_emails = args['cc_emails']
    subject = args['subject']
    body = args['body']
    dataframe = args.get('dataframe')
    excel_filename = args.get('excel_filename')
    max_retries = args.get('max_retries', 3)

    smtp_server = 'smtp.gmail.com'
    smtp_port = 587

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = to_emails
    message['Cc'] = cc_emails
    message['Subject'] = subject
    message.attach(MIMEText(body, 'html'))

    if dataframe is not None:
        try:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                dataframe.to_excel(writer, index=False)
            buffer.seek(0)
            excel_part = MIMEApplication(buffer.read(), _subtype='xlsx')
            attachment_filename = excel_filename or 'report.xlsx'
            excel_part.add_header(
                'Content-Disposition', 'attachment', filename=attachment_filename
            )
            message.attach(excel_part)
        except Exception as e:
            logging.info(f'Failed to attach DataFrame as Excel: {e}')
            body += '** FAILED TO ATTACH DATAFRAME **'

    recipients = to_emails.split(',') + cc_emails.split(',')

    for attempt in range(max_retries):
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(message, from_addr=sender_email, to_addrs=recipients)
            logging.info(
                f"Email sent to {', '.join(recipients)} at "
                f"{datetime.datetime.now(pytz.timezone('Europe/Stockholm')).strftime('%H:%M')}"
            )
            break
        except Exception as e:
            logging.info(f'Attempt {attempt + 1} failed: {e}')
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                logging.info(f'Failed to send email after {max_retries} attempts')

    return "Email activity completed"

# -------------------------------------------------------------------------
# Activity: Create DI Session (same as original)
# -------------------------------------------------------------------------
@app.activity_trigger(input_name="args")
def create_di_session(args: dict):
    DI_USERNAME = args['DI_USERNAME']
    DI_PASSWORD = args['DI_PASSWORD']

    session = requests.Session()
    login_url = "https://app.di.no/app/api/login"
    payload = {'username': DI_USERNAME, 'password': DI_PASSWORD}
    response = session.post(login_url, data=payload, timeout=120)
    if response.status_code == 200 and "topnavigation" in response.text:
        logging.info("Login successful")
        return session.cookies.get_dict()  # Return cookies so that we can rebuild a session later
    else:
        raise Exception("Login failed")

# -------------------------------------------------------------------------
# Activities for Getting Areas, Groups, Parcel Data, etc.
# -------------------------------------------------------------------------
@app.activity_trigger(input_name="args")
def get_all_areas_activity(args: dict):
    branch_emails = args['branch_emails']
    cookies = args['cookies']

    session = requests.Session()
    session.cookies.update(cookies)
    url = 'https://app.di.no/control/api/v2/geography/areas'
    response = session.get(url, headers={"Content-Type": "application/json"}, timeout=120)
    
    data = response.json()
    branch_areas_dict = {}
    for item in data:
        text = item.get('text', '').strip()
        if ' ' not in text:
            logging.warning(f"Unexpected 'text' format (no space found): '{text}'. Skipping.")
            continue
        _, branch_name = text.split(' ', 1)
        branch_name = branch_name.upper().strip()
        if branch_name in branch_emails:
            area_id = f"area{item['id']}"
            branch_areas_dict[branch_name] = area_id
    
    logging.info("Successfully retrieved areas")
    return branch_areas_dict

@app.activity_trigger(input_name="args")
def get_product_groups_activity(args: dict):
    cookies = args['cookies']
    areas_str = args['areas_str']

    session = requests.Session()
    session.cookies.update(cookies)
    url = 'https://app.di.no/control/api/v2/parcelDeviation/productgroups'
    headers = {
        "content-type": "application/json",
        "x-geography": areas_str
    }
    response = session.get(url, headers=headers, timeout=120)
    if response.status_code == 200:
        logging.info("Successfully retrieved groups")
        data = response.json()
        product_groups_list = [pg['id'] for pg in data]
        return product_groups_list
    else:
        raise Exception("Failed to retrieve product groups")

@app.activity_trigger(input_name="args")
def get_all_parcels_data_activity(args: dict):
    cookies = args['cookies']
    product_group_list = args['product_group_list']
    areas_str = args['areas_str']

    session = requests.Session()
    session.cookies.update(cookies)

    now = datetime.datetime.now(pytz.timezone('Europe/Stockholm'))
    today_date_iso = now.date().isoformat()
    url = (
        "https://app.di.no/control/api/v2/parcelDeviation/"
        f"{today_date_iso}?productGroupIds={','.join(map(str, product_group_list))}"
    )
    headers = {
        "content-type": "application/json",
        "x-geography": areas_str
    }
    response = session.get(url, headers=headers, timeout=120)
    if response.status_code == 200:
        logging.info("Successfully retrieved parcel data")
        return response.json()
    else:
        raise Exception("Failed to retrieve parcel data")

# Simple local helpers (not activities) to filter parcels
def get_not_delivered_parcels(all_parcels_data):
    return [parcel for parcel in all_parcels_data if parcel['status'] == 'PARCEL_NOT_DELIVERED']

def get_all_collected_parcels_data(all_parcels_data):
    collected = [p for p in all_parcels_data if p['status'] == 'RETURN_PARCEL_COLLECTED']
    not_collected = [p for p in all_parcels_data if p['status'] == 'RETURN_PARCEL_NOT_FOUND']
    return collected, not_collected

@app.activity_trigger(input_name="args")
def get_log_data_single_parcel_activity(args: dict):
    cookies = args['cookies']
    one_time_deliver_id = args['one_time_deliver_id']
    area_id = args['area_id']

    session = requests.Session()
    session.cookies.update(cookies)

    url = f"https://app.di.no/control/api/v2/parcelDeviation/parcel/{one_time_deliver_id}"
    headers = {"content-type": "application/json", "x-geography": area_id}
    response = session.get(url, headers=headers, timeout=120)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to get parcel log data")

def get_latest_carrier_events(parcel_data):
    from itertools import groupby

    cutoff_hour = 15
    log_events = parcel_data.get('logEvents', [])
    carrier_events = [e for e in log_events if e.get('name') == 'Carrier']

    for event in carrier_events:
        event['dateTime'] = datetime.datetime.strptime(event['dateTime'], '%Y-%m-%dT%H:%M:%S')

    def get_adjusted_date(dt):
        cutoff_time = datetime.time(hour=cutoff_hour)
        if dt.time() < cutoff_time:
            return dt.date()
        else:
            return (dt + relativedelta(days=1)).date()

    for event in carrier_events:
        event['adjusted_date'] = get_adjusted_date(event['dateTime'])

    carrier_events.sort(key=lambda x: (x['adjusted_date'], -x['dateTime'].timestamp()))

    latest_carrier_events = []
    for adj_date, group_of_events in groupby(carrier_events, key=lambda x: x['adjusted_date']):
        latest_carrier_events.append(next(group_of_events))

    for event in latest_carrier_events:
        event.pop('adjusted_date', None)
        event['dateTime'] = event['dateTime'].strftime('%Y-%m-%dT%H:%M:%S')

    parcel_data['logEvents'] = latest_carrier_events
    return parcel_data

@app.activity_trigger(input_name="args")
def get_early_bird_token_activity(args: dict):
    username = args['username']
    password = args['password']

    session = requests.Session()
    login_url = "https://api.mtd.se/users/authenticate"
    payload = {'username': username, 'password': password}
    response = session.post(login_url, json=payload, timeout=120)
    if response.status_code == 200:
        logging.info("Early Bird login successful")
        return response.json()['token']
    else:
        raise Exception("Early Bird login failed")

@app.activity_trigger(input_name="args")
def fetch_morgonexpressen_parcels_activity(args: dict):
    """
    Fetches shipment data from Early Bird API with pagination.
    """
    token = args['token']
    pagesize = args.get('pagesize', 200)
    sort = args.get('sort', '-barcode')
    start_page = args.get('start_page', 1)

    logging.info("Fetching data from Early Bird...")
    shipments = []
    current_page = start_page
    total_pages = None

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json, text/plain, */*'
    }

    today_date = datetime.datetime.now(pytz.timezone('Europe/Stockholm')).date()
    from_date = (today_date - relativedelta(days=5)).isoformat()
    to_date = (today_date + relativedelta(days=5)).isoformat()

    while True:
        url = (
            f"https://api.mtd.se/shipmentsfiltered?pagesize={pagesize}&sort={sort}"
            f"&FromDate={from_date}&ToDate={to_date}&page={current_page}"
        )
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                results = data.get('results', [])
                shipments.extend(results)

                if total_pages is None:
                    total_pages = data.get('pagecount', 1)

                if current_page >= total_pages:
                    break
                else:
                    current_page += 1
            else:
                logging.exception(f"Failed to fetch data: {resp.status_code} - {resp.text}")
                resp.raise_for_status()
        except Exception as e:
            logging.exception(f"Error fetching shipments: {e}")
            raise

    logging.info("Data from Early Bird is fetched.")
    return [s['barCode'] for s in shipments]

# -------------------------------------------------------------------------
# Activity: Process Parcels (the big difference)
# -------------------------------------------------------------------------
@app.activity_trigger(input_name="args")
def process_parcels_activity(args: dict) -> list:
    """
    A single Activity that processes a list of parcels. We do the requests
    with a per-request timeout of 10 seconds to avoid hangs. If an error
    occurs, we raise an exception so the orchestrator can retry or handle it.

    Returns:
        list of dict: automation_result_list for each parcel, so the orchestrator
        can gather the statuses.
    """
    cookies = args["cookies"]
    parcels = args["parcels"]           # e.g. [parcel0, parcel1, ...]
    event_type_description = args["event_type_description"]
    post_url = args["post_url"]

    # Additional fields if needed, for example next_delivery_date
    next_delivery_date_str = args.get("next_delivery_date_str")

    logging.info(f"ProcessParcels - event='{event_type_description}' - count={len(parcels)}")

    # Rebuild session
    session = requests.Session()
    session.cookies.update(cookies)

    automation_result_list = []

    for parcel in parcels:
        area = parcel.get('area', {})
        area_id = f"area{str(area.get('areaId', ''))}"
        branch_name = area.get('areaName', '')
        route = parcel.get('route', {})
        route_name = route.get('routeName', '')
        tracking_number = str(parcel.get('trackingNumber', ''))

        headers = {
            "content-type": "application/json",
            "x-geography": area_id
        }

        # Build the payload logic:
        if event_type_description == "Ny visning":
            # We need the next delivery date
            payload = {
                "directionAdvice": "",
                "oneTimeDeliveryId": str(parcel.get('oneTimeDeliverId', '')),
                "retryDate": next_delivery_date_str
            }
        elif event_type_description == "Retur":
            payload = {
                "oneTimeDeliveryId": str(parcel.get('oneTimeDeliverId', '')),
                "parcelFollowupId": "25"
            }
        elif event_type_description == "Förlorat":
            payload = {
                "oneTimeDeliveryId": str(parcel.get('oneTimeDeliverId', '')),
                "parcelFollowupId": ""
            }
        elif event_type_description == "Upphämtning":
            dt_now = datetime.datetime.now(pytz.timezone('Europe/Stockholm'))
            payload = {
                "oneTimeDeliveryId": str(parcel.get('oneTimeDeliverId', '')),
                "parcelFollowupId": "33",
                "serviceActivationDate": dt_now.date().isoformat(),
                "serviceActivationTime": dt_now.strftime('%H:%M')
            }
        else:
            logging.warning(f"Unknown event_type_description={event_type_description}; skipping {tracking_number}")
            continue

        result_data = {
            'branch_name': branch_name,
            'route_name': route_name,
            'tracking_number': tracking_number,
            'event_type_description': event_type_description,
            'Status': None
        }

        try:
            # Use a short-ish timeout to avoid indefinite hangs
            response = session.post(post_url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            logging.info(f"{tracking_number}: {event_type_description} - OK")
            result_data['Status'] = 'OK'
        except Exception as e:
            logging.exception(f"{tracking_number}: {event_type_description} - rejected => {e}")
            result_data['Status'] = 'rejected'
            # We could choose to raise after first fail, or just keep going
            # If we want the whole activity to fail on the first error:
            raise

        automation_result_list.append(result_data)
        time.sleep(1.0)  # Sleep 1s to avoid spamming server

    return automation_result_list

# -------------------------------------------------------------------------
# Helper for building DataFrame from partial results
# -------------------------------------------------------------------------
def create_parcels_dataframe(parcels_list):
    columns = ['Filial', 'Traktnummer', 'kundId', 'Sista Status']
    data = []
    for item in parcels_list:
        if isinstance(item, list) and len(item) == 2:
            parcel, log_event = item
            additional_info = log_event.get('additionalInfoDescription', '')
        elif isinstance(item, dict):
            # Possibly from the 'collected' or direct approach
            parcel = item
            additional_info = parcel.get('status', '')
        else:
            continue

        area = parcel.get('area', {})
        branch_name = area.get('areaName', '')
        route = parcel.get('route', {})
        route_name = route.get('routeName', '')
        tracking_number = str(parcel.get('trackingNumber', ''))

        data.append({
            'Filial': branch_name,
            'Traktnummer': route_name,
            'kundId': tracking_number,
            'Sista Status': additional_info
        })

    df = pd.DataFrame(data, columns=columns)
    status_mapping = {
        '. Bud har ikke mottatt pakken': 'Ej mottagen',
        '. Pakken er skadet': 'Skadad',
        '. Mangler nøkkel': 'Saknar nyckel',
        'Finner ikke kundens leveringspunkt': 'Hittar inte kund',
        '. Uegnet leveringspunkt': 'Olämplig leveransplats',
        'RETURN_PARCEL_COLLECTED': 'Paket upphämtat'
    }
    df['Sista Status'] = df['Sista Status'].replace(status_mapping)
    df = df.sort_values(by='Traktnummer', ascending=True).reset_index(drop=True)
    return df

# -------------------------------------------------------------------------
# Generator to fetch and categorize each parcel's log data
# -------------------------------------------------------------------------
@app.activity_trigger(input_name="args")
def categorize_parcels_for_action_activity(args: dict):
    """
    Runs completely inside 1 activity worker – no Durable APIs here.
    Inputs
        cookies          : DI session cookies (dict[str, str])
        not_delivered    : list[dict]  – raw parcel json objects
        morgon_list      : set[str]    – barCodes that must be returned
        delivery_tries   : int         – how many 'key missing / bad point'
                                         attempts allowed before we return a parcel
    Returns
        (extend_list, return_list, lost_list)
        where each item is [parcel, latest_carrier_event]
    """
    cookies_dict         = args["cookies"]
    not_delivered_parcel_data   = args["not_delivered_parcel_data"]
    morgonexpressen_parcels     = set(args["morgonexpressen_parcels"])
    delivery_tries  = args.get("delivery_tries", 2)

    parcels_extend_list = []
    parcels_return_list = []
    parcels_lost_list = []

    # build a real session that can perform requests
    sess = requests.Session()
    sess.cookies.update(cookies_dict)

    def fetch_log(one_time_deliver_id, area_id):
        url     = f"https://app.di.no/control/api/v2/parcelDeviation/parcel/{one_time_deliver_id}"
        r = sess.get(url, 
                        headers={"content-type": "application/json","x-geography": area_id}, 
                        timeout=30)
        r.raise_for_status()
        return r.json()

    for parcel in not_delivered_parcel_data:
        one_time_deliver_id = parcel['oneTimeDeliverId']
        area_id = f"area{str(parcel['area']['areaId'])}"
        tracking_number = str(parcel['trackingNumber'])
        
        log_data_single_parcel = fetch_log(one_time_deliver_id, area_id)
        
        if log_data_single_parcel is None:
            continue

        log_data_filtered_last_ts = get_latest_carrier_events(log_data_single_parcel)
        
        if 'logEvents' in log_data_filtered_last_ts:
            log_events = log_data_filtered_last_ts['logEvents']

            # Sort log events by dateTime in descending order to check the latest event
            log_events.sort(key=lambda x: datetime.datetime.fromisoformat(x['dateTime']), reverse=True)
            
            # Condition for one delivery try for Morgonexpressen parcels
            if tracking_number in morgonexpressen_parcels:
                parcels_return_list.append([parcel, log_events[0]])
                continue

            # Condition for damaged parcels
            if ". Pakken er skadet" in log_events[0].get('additionalInfoDescription', ''):
                parcels_return_list.append([parcel, log_events[0]])
                continue
            
            # Condition for lost parcels  
            if ". Bud har ikke mottatt pakken" in log_events[0].get('additionalInfoDescription', ''):
                parcels_lost_list.append([parcel, log_events[0]])
                continue
            
            # Count occurrences of specific phrases in log events
            count_mangler_nokkel = sum(
                '. Mangler nøkkel' in event.get('additionalInfoDescription', '') for event in log_events
            )
            count_finner_ikke = sum(
                'Finner ikke kundens leveringspunkt' in event.get('additionalInfoDescription', '') for event in log_events
            )
            count_uegnet_leveringspunkt= sum(
                '. Uegnet leveringspunkt' in event.get('additionalInfoDescription', '') for event in log_events
            )
            
            extend_condition = (
                (count_mangler_nokkel + count_finner_ikke + count_uegnet_leveringspunkt) < delivery_tries and
                count_mangler_nokkel < delivery_tries and
                count_finner_ikke < delivery_tries and
                count_uegnet_leveringspunkt < delivery_tries
            )

            if extend_condition:
                # Extend the parcel if the condition is met
                parcels_extend_list.append([parcel, log_events[0]])
            else:
                # Return the parcel if the condition is not met
                parcels_return_list.append([parcel, log_events[0]])

    logging.info("Successfully retrieved log data for last event")
    return parcels_extend_list, parcels_return_list, parcels_lost_list

# -------------------------------------------------------------------------
# Orchestrator Function with Try/Except
# -------------------------------------------------------------------------
@app.orchestration_trigger(context_name="context")
def orchestrator_function(context: df.DurableOrchestrationContext):
    try:
        # If we want to skip if outside time window
        # if not should_run_script():
        #     logging.info("Not in the window to run the script. Exiting orchestration.")
        #     return "Skipped"

        now = datetime.datetime.now(pytz.timezone('Europe/Stockholm'))
        today_date_iso = now.date().isoformat()

        # Gather secrets
        DI_USERNAME = client.get_secret('DI-USERNAME').value
        DI_PASSWORD = client.get_secret('DI-PASSWORD').value
        EARLY_BIRD_USERNAME = client.get_secret('EARLY-BIRD-USERNAME').value
        EARLY_BIRD_PASSWORD = client.get_secret('EARLY-BIRD-PASSWORD').value
        SENDER_EMAIL = client.get_secret("GMAIL-USERNAME").value
        SENDER_PASSWORD = client.get_secret("GMAIL-APP-PASS").value
        BRANCH_EMAILS = json.loads(client.get_secret('BRANCH-EMAILS').value)
        _TO_EMAILS_DEV = os.getenv('_TO_EMAILS_DEV', '')
        _TO_EMAILS_PROD = os.getenv('_TO_EMAILS_PROD', '')

        # Calculate next delivery date
        RED_DAYS_LIST = [d.strip() for d in client.get_secret('RED-DAYS-LIST').value.split(',') if d.strip()]
        next_delivery_date = now + relativedelta(days=1)
        next_delivery_date_str = next_delivery_date.strftime('%Y-%m-%d')
        while next_delivery_date_str in RED_DAYS_LIST:
            next_delivery_date += relativedelta(days=1)
            next_delivery_date_str = next_delivery_date.strftime('%Y-%m-%d')

        retry_opts = RetryOptions(1, 3)
        automation_result_list = []

        # ── external I/O – each call MUST be yielded ────────────────────
        cookies = yield context.call_activity("create_di_session", {
            "DI_USERNAME": DI_USERNAME,
            "DI_PASSWORD": DI_PASSWORD,
        })

        branch_areas_dict = yield context.call_activity("get_all_areas_activity", {
            "branch_emails": BRANCH_EMAILS,
            "cookies": cookies,
        })
        areas_str = ",".join(branch_areas_dict.values())

        product_group_list = yield context.call_activity("get_product_groups_activity", {
            "cookies": cookies,
            "areas_str": areas_str
        })

        token = yield context.call_activity("get_early_bird_token_activity", {
            "username": EARLY_BIRD_USERNAME,
            "password": EARLY_BIRD_PASSWORD,
        })
        morgonexpressen_parcels = yield context.call_activity("fetch_morgonexpressen_parcels_activity", {
            "token": token,
        })

        # ── per-branch processing loop ──────────────────────────────────
        for branch_name, area_id in branch_areas_dict.items():

            all_parcels_data = yield context.call_activity("get_all_parcels_data_activity", {
                "cookies": cookies,
                "product_group_list": product_group_list,
                "areas_str": "area39799", #area_id
            })

            not_delivered_parcel_data = get_not_delivered_parcels(all_parcels_data)
            collected, _  = get_all_collected_parcels_data(all_parcels_data)

            (extend_list,
            return_list,
            lost_list) = yield context.call_activity(
                "categorize_parcels_for_action_activity",
                {
                    "cookies":       cookies,
                    "not_delivered_parcel_data": not_delivered_parcel_data,
                    "morgonexpressen_parcels":   morgonexpressen_parcels,
                    "delivery_tries": 2,
                },
            )

            # -- dispatch follow-up actions in four categories --
            for desc, parcel_list, url, extra in [
                ("Ny visning",  extend_list,   "https://app.di.no/control/api/v2/parcelDeviation/newDelivery",
                                          {"next_delivery_date_str": next_delivery_date_str}),
                ("Retur",       return_list,   "https://app.di.no/control/api/v2/parcelDeviation/returned",   None),
                ("Förlorat",    lost_list,     "https://app.di.no/control/api/v2/parcelDeviation/lost",       None),
                ("Upphämtning", collected,     "https://app.di.no/control/api/v2/parcelDeviation/collected",  None),
            ]:
                if not parcel_list:
                    continue

                payload = {
                    "cookies": cookies,
                    "parcels": [p[0] if isinstance(p, list) else p for p in parcel_list],
                    "event_type_description": desc,
                    "post_url": url,
                }
                if extra:
                    payload.update(extra)

                result = yield context.call_activity_with_retry(
                    "process_parcels_activity", retry_opts, payload
                )
                automation_result_list.extend(result)

            # -- branch-level status e-mail --
            summary_tables = {
                "Ny visning":  create_parcels_dataframe(extend_list),
                "Retur":       create_parcels_dataframe(return_list),
                "Förlorat":    create_parcels_dataframe(lost_list),
                "Upphämtning": create_parcels_dataframe(collected),
            }

            email_to = BRANCH_EMAILS.get(branch_name)
            if email_to:
                html_sections = "".join(
                    f"<p><b>{title}</b>:</p>{df.to_html(index=False) if not df.empty else '<p>Inga paket.</p>'}"
                    for title, df in summary_tables.items()
                )
                yield context.call_activity("send_email_with_excel_from_df", {
                    "sender_email":   SENDER_EMAIL,
                    "sender_password":SENDER_PASSWORD,
                    "to_emails":      email_to,
                    "cc_emails":      _TO_EMAILS_DEV,
                    "subject":        f"{branch_name}: Paketstatusrapport {today_date_iso}",
                    "body":           f"<html><body><h3>Paketstatusrapport för {branch_name}</h3>{html_sections}</body></html>",
                })

        # ── consolidated debug report ───────────────────────────────────
        df_automation = pd.DataFrame(automation_result_list).sort_values(by="route_name")
        yield context.call_activity("send_email_with_excel_from_df", {
            "sender_email":   SENDER_EMAIL,
            "sender_password":SENDER_PASSWORD,
            "to_emails":      _TO_EMAILS_DEV,
            "cc_emails":      _TO_EMAILS_DEV,
            "subject":        f"parcel-management-automation {today_date_iso}",
            "body": (
                f"<p><b>** DEBUG **</b></p>"
                f"<p><b>Antal hanterade paket</b>: {len(df_automation)}</p>"
                f"<p><b>Antal OK</b>:  {len(df_automation[df_automation['Status']=='OK'])}</p>"
                f"<p><b>Antal fel</b>: {len(df_automation[df_automation['Status']=='rejected'])}</p>"
            ),
            "dataframe":       df_automation.to_dict(orient="records"),   # JSON-safe
            "excel_filename": f"parcel-management-automation {today_date_iso}.xlsx",
        })

        return "Done"

    except Exception as exc:
        # single best-effort alert; no retry logic in failure path
        yield context.call_activity("send_email_with_excel_from_df", {
            "sender_email":   SENDER_EMAIL,
            "sender_password":SENDER_PASSWORD,
            "to_emails":      _TO_EMAILS_DEV,
            "cc_emails":      _TO_EMAILS_DEV,
            "subject":        f"[FAIL] parcel-management-automation failed {today_date_iso}",
            "body":           f"<p>{exc}</p>",
        })
        raise


# -------------------------------------------------------------------------
# Timer Trigger That Starts the Orchestration
# -------------------------------------------------------------------------
@app.durable_client_input(client_name="starter")  
@app.timer_trigger(
    schedule="0 0 10,11 * * *",
    arg_name="myTimer",
    run_on_startup=True,
    use_monitor=True
)
async def timer_trigger(myTimer: func.TimerRequest, starter: df.DurableOrchestrationClient) -> None:
    """
    Timer Trigger that starts the Orchestrator. The code inside the orchestrator
    is broken into smaller activity calls, preventing silent shutdown from
    killing the entire run.
    """
    if myTimer.past_due:
        logging.info('The timer is past due!')

    # Just start the orchestrator. The orchestrator will handle the logic.
    instance_id = await starter.start_new("orchestrator_function", None)
    logging.info(f"Started orchestration with ID = '{instance_id}'")
