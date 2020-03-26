from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.authentication import BasicAuthentication
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from drf_yasg.utils import swagger_auto_schema
from cccatalog.api.models import Image
from cccatalog.api.utils.view_count import track_model_views
from cccatalog.api.serializers.image_serializers import\
    ImageSerializer, OembedImageSerializer
from cccatalog.settings import THUMBNAIL_PROXY_URL
import cccatalog.api.controllers.search_controller as search_controller
from cccatalog.api.utils.exceptions import input_error_response
import logging

log = logging.getLogger(__name__)

FORMAT = 'format'
URL = 'url'

class OembedImageDetail(GenericAPIView):
    authentication_classes = [BasicAuthentication]
    permission_classes = [IsAuthenticatedOrReadOnly]

    @swagger_auto_schema(operation_id="oembed_image_detail",
                         operation_description="Load the details of a"
                                               " particular image ID through oembed.",
                         responses={
                             200: OembedImageSerializer,
                             404: 'Not Found'
                         })
    @track_model_views(Image)
    def get(self, request, format=None, view_count=0):
        format = request.query_params[FORMAT]
        url = request.query_params[URL]
        identifier = self.parse_id(url)
        try:
            queryset = Image.objects.get(identifier=identifier)
            serialized_image = OembedImageSerializer(queryset).data
            return Response(status=200, data=serialized_image)
        except Image.DoesNotExist:
            queryset = None
            return Response(status=404, data='Not found')


    def parse_id(self,url):
        splits = url.split('/')
        return(splits.pop())
