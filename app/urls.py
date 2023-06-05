from django.urls import path
from .views import trigger_report, get_report

urlpatterns = [
    path("trigger_report", trigger_report, name="trigger-report"),
    path("get_report", get_report, name="get_report"),
]
