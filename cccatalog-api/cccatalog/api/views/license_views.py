from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.views import APIView

from cccatalog.api.licenses import LICENSES, LICENSE_URL

class ListLicense(APIView):
    """
    List all licenses available in catalog
    """
    @swagger_auto_schema(operation_id='license')
    def get(self, request):
        response = []
        for license in LICENSES:
          response.append({'license': license[0], 'license_version': 2, 'url':
          LICENSE_URL})

        return Response(data=response)
