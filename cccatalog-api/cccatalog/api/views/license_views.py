from django.core import serializers
from django.http import HttpResponse
from django.views import View
from django.views.generic import ListView
from drf_yasg.utils import swagger_auto_schema
from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework.views import APIView

from cccatalog.api.models import Image
from cccatalog.api.serializers.license_serializers import LicenseSerializer

class ListLicense(APIView):
    """
    List all licenses available in catalog
    """
    @swagger_auto_schema(operation_id='license',
                         query_serializer=LicenseSerializer,
                         responses={
                             200: LicenseSerializer(many=True)
                         })
    def get(self, request):
        images = Image.objects.order_by('license', 'license_version').distinct('license', 'license_version')
        serialized_image = LicenseSerializer(images, many=True)
        return Response(data=serialized_image.data)
