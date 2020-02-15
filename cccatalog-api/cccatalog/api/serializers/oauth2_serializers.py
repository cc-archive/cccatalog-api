from rest_framework import serializers
from cccatalog.api.models import OAuth2Registration
from oauth2_source.models import Application


class OAuth2RegistrationSerializer(serializers.ModelSerializer):
    class Meta:
        model = OAuth2Registration
        fields = ('name', 'description', 'email')


class OAuth2RegistrationSuccessful(serializers.ModelSerializer):
    class Meta:
        model = Application
        fields = ('name', 'client_id', 'client_secret')


class OAuth2KeyInfo(serializers.Serializer):
    requests_this_minute = serializers.IntegerField(
        help_text="The number of requests your key has performed in the last "
                  "minute.",
        allow_null=True
    )
    requests_today = serializers.IntegerField(
        help_text="The number of requests your key has performed in the last "
                  "day.",
        allow_null=True
    )
    rate_limit_model = serializers.CharField(
        help_text="The type of rate limit applied to your key. Can be "
                  "'standard' or 'enhanced'; enhanced users enjoy higher rate "
                  "limits than their standard key counterparts. Contact "
                  "Creative Commons if you need a higher rate limit."
    )
