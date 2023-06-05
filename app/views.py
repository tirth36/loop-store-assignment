import secrets
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.status import (
    HTTP_200_OK,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_400_BAD_REQUEST,
)
from app import logic
from django.http import HttpResponse
from django.core.exceptions import ObjectDoesNotExist


# Create your views here.
@api_view(["POST"])
def trigger_report(request):
    try:
        report_id = secrets.token_urlsafe(16)
        report = logic.generate_report(report_id)
        # print(report)
        return Response(
            {"report_id": report_id,  "status": HTTP_200_OK, "error": None}
        )
    except Exception as e:
        return Response(
            {
                "error": "Something went Wrong",
                "status": HTTP_500_INTERNAL_SERVER_ERROR,
                "error_desc": str(e),
            }
        )


@api_view(["GET"])
def get_report(request):
    try:
        report_id = request.query_params.get("report_id")
        if not report_id:
            return Response(
                {"error": "Report ID cannot be blank", "status": HTTP_400_BAD_REQUEST}
            )

        report_status = logic.get_report_status_from_db(report_id)
        if not report_status:
            return Response(
                {"error": "Invalid report ID", "status": HTTP_400_BAD_REQUEST}
            )

        if report_status == "Running":
            return Response(
                {"report_status": "Running", "status": HTTP_200_OK, "error":None}
            )
        elif report_status == "Complete":
            report_data = logic.get_report_data_from_db(report_id)
            if report_data:
                return HttpResponse(report_data, content_type="text/csv")
            else:
                return Response(
                    {
                        "error": "Failed to retrieve report data",
                        "status": HTTP_400_BAD_REQUEST,
                    }
                )
        else:
            return Response(
                {"error": "Invalid report status", "status": HTTP_400_BAD_REQUEST}
            )
    except Exception as e:
        return Response(
            {
                "error": "Something went wrong",
                "status": HTTP_500_INTERNAL_SERVER_ERROR,
                "error_desc": str(e),
            }
        )
