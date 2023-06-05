from django.db import models
from django.utils import timezone


# Create your models here.
class Store(models.Model):
    timestamp_utc = models.DateTimeField(auto_now=False, auto_now_add=False)
    status = models.CharField(max_length=50)


class BusinessDuration(models.Model):
    store_id = models.IntegerField()
    day_of_week = models.SmallIntegerField()
    start_time_local = models.TimeField(auto_now=False, auto_now_add=False)
    end_time_local = models.TimeField(auto_now=False, auto_now_add=False)


class StoreTimezone(models.Model):
    store_id = models.IntegerField()
    timezone_str = models.CharField(max_length=255)


class Report(models.Model):
    report_id = models.CharField(max_length=255, unique=True)
    status = models.CharField(max_length=50)
    data = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, default=None)
