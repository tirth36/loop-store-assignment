import json
from app.models import Store, Report
from app.utils import calculate_uptime
from datetime import datetime


def generate_report(report_id):
    report = Report(report_id=report_id, status="Running", data="")
    report.save()
    report_data = []
    stores = Store.objects.all()[:100]
    for store in stores:
        uptime, downtime = calculate_uptime(store.id)
        report_data.append(
            {
                "store_id": store.id,
                "status": store.status,
                "uptime": round(uptime, 2),
                "downtime": round(downtime, 2),
            }
        )

    report.status = "Complete"
    report.completed_at = datetime.utcnow()

    report.data = json.dumps(report_data)
    return report


def get_report_status_from_db(report_id):
    report = Report.objects.filter(report_id=report_id).first()
    if report is None:
        return None
    else:
        return report.status


def get_report_data_from_db(report_id):
    report = Report.objects.filter(report_id=report_id).first()

    if report is None:
        raise ValueError(f"No report found for report_id: {report_id}")

    return report.data
