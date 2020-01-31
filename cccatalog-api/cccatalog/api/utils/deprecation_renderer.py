from rest_framework.renderers import JSONRenderer
from rest_framework.utils.serializer_helpers import ReturnDict


deprecation_message = '''You are using a legacy version of the CC \
Catalog API. Please upgrade to version 1.0.0 before July 31st 2020, \
after which we plan to shut down the legacy version. See the most \
recent API documentation at https://api.creativecommons.engineering/v1/ for \
details on the new API.'''


class DeprecationJSONRenderer(JSONRenderer):
    """
    Adds a deprecation warning to all JSON responses.
    """
    def render(self, data, accepted_media_type=None, renderer_context=None):
        allowed_types = set([dict, ReturnDict])
        if type(data) in allowed_types:
            data['warning'] = deprecation_message
        response = super(DeprecationJSONRenderer, self).render(
            data, accepted_media_type, renderer_context
        )
        return response
