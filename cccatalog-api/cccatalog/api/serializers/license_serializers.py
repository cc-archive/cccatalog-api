from rest_framework import serializers


class LicenseSerializer(serializers.Serializer):
    license_version = serializers.CharField()
    license_url = serializers.CharField()
    jurisdiction = serializers.CharField()
    language_code = serializers.CharField()