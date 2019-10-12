from rest_framework import serializers
from cccatalog.api.models import Image


class LicenseSerializer(serializers.Serializer):
    license = serializers.CharField(help_text="License name")
    license_version = serializers.CharField(help_text="License version")

    class Meta:
        model = Image
        fields = ['license', 'license_version']
