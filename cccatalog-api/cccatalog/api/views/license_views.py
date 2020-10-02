from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from cccatalog.api.licenses import parse_rdf_cache_license
from rest_framework.response import Response
from django.core.cache import cache
from cccatalog.api.serializers.license_serializers import LicenseSerializer


class LicenseView(APIView):
    """
    List all licenses available in catalog
    """
    @swagger_auto_schema(operation_id='licenses',
                         query_serializer=LicenseSerializer,
                         responses={
                             200: LicenseSerializer(many=True),
                         })
    def get(self, request):
        parse_rdf_cache_license(
            "https://creativecommons.org/licenses/index.rdf")
        licenses_response = cache.get(key='licenses')
        serialized_licenses = LicenseSerializer(
            data=licenses_response, many=True)
        return Response(status=200, data=serialized_licenses.initial_data)
