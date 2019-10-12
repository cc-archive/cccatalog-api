"""cccatalog URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, re_path
from django.conf.urls import include
from cccatalog.api.views.image_views import SearchImages, ImageDetail,\
    Watermark, BrowseImages, RelatedImage
from cccatalog.api.views.license_views import ListLicense
from cccatalog.api.views.site_views import HealthCheck, ImageStats, Register, \
    CheckRates, VerifyEmail
from cccatalog.api.views.link_views import CreateShortenedLink, \
    ResolveShortenedLink
from cccatalog.settings import API_VERSION, WATERMARK_ENABLED
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
import rest_framework.permissions

description = """
The Creative Commons Catalog API ('cccatalog-api') is a system
that allows programmatic access to public domain digital media. It is our
ambition to index and catalog billions of Creative Commons works, including
articles, songs, videos, photographs, paintings, and more. Using this API,
developers will be able to access the digital commons in their own
applications.

Please note that there is a rate limit of 5000 requests per day and
60 requests per minute rate limit in place for anonymous users. This is fine
for introducing yourself to the API, but we strongly recommend that you obtain
an API key as soon as possible. Authorized clients have a higher rate limit
of 10000 requests per day and 100 requests per minute. Additionally, Creative
Commons can give your key an even higher limit that fits your application's
needs. See the `/oauth2/register` endpoint for instructions on obtaining
an API key.

Pull requests are welcome!
[Contribute on GitHub](https://github.com/creativecommons/cccatalog-api)
"""


logo_url = "https://mirrors.creativecommons.org/presskit/logos/cc.logo.svg"
tos_url = "https://api.creativecommons.engineering/terms_of_service.html"
license_url =\
    "https://github.com/creativecommons/cccatalog-api/blob/master/LICENSE"
schema_view = get_schema_view(
    openapi.Info(
        title="Creative Commons Catalog API",
        default_version=API_VERSION,
        description=description,
        contact=openapi.Contact(email="cccatalog-api@creativecommons.org"),
        license=openapi.License(name="MIT License", url=license_url),
        terms_of_service=tos_url,

        x_logo={
            "url": logo_url,
            "backgroundColor": "#FFFFFF"
        }
    ),
    public=True,
    permission_classes=(rest_framework.permissions.AllowAny,),
)

urlpatterns = [
    path('', schema_view.with_ui('redoc', cache_timeout=None), name='root'),
    path('admin/', admin.site.urls),
    path('oauth2/register', Register.as_view(), name='register'),
    path('oauth2/key_info', CheckRates.as_view(), name='key_info'),
    path(
        'oauth2/verify/<str:code>',
        VerifyEmail.as_view(),
        name='verify-email'
    ),
    re_path(
        r'^oauth2/',
        include('oauth2_provider.urls', namespace='oauth2_provider')
    ),
    # path('list', CreateList.as_view()),
    # path('list/<str:slug>', ListDetail.as_view(), name='list-detail'),
    re_path('image/search', SearchImages.as_view()),
    path('image/browse/<str:provider>', BrowseImages.as_view()),
    path('image/<str:identifier>', ImageDetail.as_view(), name='image-detail'),
    path(
        'image/related/<str:identifier>',
        RelatedImage.as_view(),
        name='related-images'
    ),
    path('statistics/image', ImageStats.as_view(), name='about-image'),
    path('link', CreateShortenedLink.as_view(), name='make-link'),
    path('link/<str:path>', ResolveShortenedLink.as_view(), name='resolve'),
    re_path('healthcheck', HealthCheck.as_view()),
    re_path(
        r'^swagger(?P<format>\.json|\.yaml)$',
        schema_view.without_ui(cache_timeout=None), name='schema-json'
    ),
    re_path(
        r'^swagger/$',
        schema_view.with_ui('swagger', cache_timeout=15),
        name='schema-swagger-ui'
    ),
    re_path(
        r'^redoc/$',
        schema_view.with_ui('redoc', cache_timeout=15),
        name='schema-redoc'
    ),
    path('license/', ListLicense.as_view())
]

if WATERMARK_ENABLED:
    urlpatterns.append(path('watermark/<str:identifier>', Watermark.as_view()))
