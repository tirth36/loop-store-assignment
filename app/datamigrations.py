# app/services.py

import threading
import time
import functools
import itertools
import pandas as pd
import pytz
from app.models import BusinessDuration, Store, StoreTimezone
from datetime import datetime, timedelta


def migrate_data():
    batch_size = 500

    read_stores = pd.read_csv("dataset/store status.csv", chunksize=batch_size)
    read_business_duration = pd.read_csv("dataset/Menu hours.csv", chunksize=batch_size)
    read_timezones = pd.read_csv("dataset/timezoneinfo.csv", chunksize=batch_size)

    # Define timezone dictionary
    timezone_dict = {}
    store_objs = []
    business_duration_objs = []
    timezone_objs = []

    for i, row in pd.concat(read_timezones).iterrows():
        timezone_dict[row["store_id"]] = pytz.timezone(row["timezone_str"])
        timezone_obj = StoreTimezone(
            store_id=row["store_id"], timezone_str=row["timezone_str"]
        )
        timezone_obj.save()

    # Insert data into the database
    for df_store in read_stores:
        df_store = df_store.dropna(subset=["timestamp_utc"])
        df_store["timestamp_utc"] = pd.to_datetime(df_store["timestamp_utc"])

    for i, row in df_store.iterrows():
        store_id = row["store_id"]
        status = row["status"]
        timezone = timezone_dict.get(store_id, pytz.timezone("America/Chicago"))
        timestamp_local = row["timestamp_utc"].astimezone(timezone)
        store = Store(timestamp_utc=row["timestamp_utc"], status=status)
        store.save()

    # while True:
    #     batch = list(itertools.islice(store_objs, batch_size))
    #     if not batch:
    #         break
    #     Store.objects.bulk_create(batch, batch_size)

    for df_business_duration in read_business_duration:
        for i, row in df_business_duration.iterrows():
            start_time = pd.to_datetime(row["start_time_local"]).time()
            end_time = pd.to_datetime(row["end_time_local"]).time()
            business_duration = BusinessDuration(
                store_id=row["store_id"],
                day_of_week=row["day"],
                start_time_local=start_time,
                end_time_local=end_time,
            )
            business_duration.save()
    # business_duration_objs.append(business_duration)

    # batches = [
    #     business_duration_objs[j : j + batch_size]
    #     for j in range(0, len(business_duration_objs), batch_size)
    # ]
    # for batch in batches:
    #     BusinessDuration.objects.bulk_create(batch)

    # for timezones_df in read_timezones:
    #     print(timezones_df)
    #     for i, row in timezones_df.iterrows():
    #         timezone_obj = StoreTimezone(
    #             store_id=row["store_id"], timezone_str=row["timezone_str"]
    #         )
    #         timezone_obj.save()
    # batches = [
    #     timezone_objs[j : j + batch_size]
    #     for j in range(0, len(timezone_objs), batch_size)
    # ]
    # for batch in batches:
    #     StoreTimezone.objects.bulk_create(batch)


def execute_data_migration():
    migrate_data()


# t = threading.Thread(target=execute_data_migration)
# t.start()


def refresh_cache():
    while True:
        time.sleep(3600)
        execute_data_migration()


# # start the refresh_cache() function in a separate thread
# t2 = threading.Thread(target=refresh_cache)
# t2.start()


def get_store_uptime_downtime(store_id, start_date, end_date):
    """
    Computes the uptime and downtime for a given store within a given time range.
    """
    store = Store.query.get(store_id)

    # Retrieve the store's timezone
    timezone_str = StoreTimezone.objects.filter(store_id=store_id).first().timezone_str
    timezone = pytz.timezone(timezone_str)

    # Compute business hours in local timezone for each day within the given time range
    business_hours = {}
    for day_offset in range((end_date - start_date).days + 1):
        date = start_date + timedelta(days=day_offset)
        day_of_week = date.weekday()
        local_start_time = datetime.combine(
            date,
            BusinessDuration.objects.filter(store_id=store_id, day_of_week=day_of_week)
            .first()
            .start_time_local,
        )
        local_end_time = datetime.combine(
            date,
            BusinessDuration.objects.filter(store_id=store_id, day_of_week=day_of_week)
            .first()
            .end_time_local,
        )
        business_hours[date] = (
            local_start_time.astimezone(timezone),
            local_end_time.astimezone(timezone),
        )

    # Retrieve store status changes within the given time range
    status_changes = (
        store.status_changes.filter(Store.timestamp_utc.between(start_date, end_date))
        .order_by(Store.timestamp_utc)
        .all()
    )

    # Initialize counters for uptime and downtime
    uptime = timedelta()
    downtime = timedelta()

    # Compute uptime and downtime based on status changes and business hours
    last_status = None
    for i, status_change in enumerate(status_changes):
        if i == 0:
            last_status = status_change.status
            continue

        time_diff = status_change.timestamp_utc - status_changes[i - 1].timestamp_utc

        if last_status == "open":
            # Compute downtime during non-business hours
            for j in range(
                (status_changes[i - 1].timestamp_utc.date() - start_date).days,
                (status_change.timestamp_utc.date() - start_date).days,
            ):
                date = start_date + timedelta(days=j)
                if business_hours[date][1] < business_hours[date][0]:
                    downtime += timedelta(hours=24) - (
                        business_hours[date][1] - business_hours[date][0]
                    )
                else:
                    downtime += max(
                        timedelta(), business_hours[date][0] - business_hours[date][1]
                    )
            uptime += time_diff
        else:
            # Compute uptime during business hours
            for j in range(
                (status_changes[i - 1].timestamp_utc.date() - start_date).days,
                (status_change.timestamp_utc.date() - start_date).days,
            ):
                date = start_date + timedelta(days=j)
                if business_hours[date][1] < business_hours[date][0]:
                    uptime += timedelta(hours=24) - (
                        business_hours[date][1] - business_hours[date][0]
                    )
                else:
                    uptime += max(
                        timedelta(), business_hours[date][1] - business_hours[date][0]
                    )
            downtime += time_diff

        last_status = status_change.status

    # Compute uptime and downtime for the last status change to the end of the time range
    # Compute uptime and downtime for the last status change to the end of the time range
    if last_status == "open":
        for j in range(
            (status_changes[-1].timestamp_utc.date() - start_date).days,
            (end_date - start_date).days + 1,
        ):
            date = start_date + timedelta(days=j)
            if business_hours[date][1] is not None:
                downtime += business_hours[date][1] - business_hours[date][0]

    else:
        for j in range(
            (status_changes[-1].timestamp_utc.date() - start_date).days,
            (end_date - start_date).days + 1,
        ):
            date = start_date + timedelta(days=j)
            if business_hours[date][0] is not None:
                uptime += business_hours[date][1] - business_hours[date][0]

    return uptime, downtime
