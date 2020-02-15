from uuslug import uuslug
from django.db import models
from django.utils.safestring import mark_safe
from django.contrib.postgres.fields import JSONField, ArrayField
from cccatalog.api.licenses import ATTRIBUTION, get_license_url
from oauth2_provider.models import AbstractApplication


class OpenLedgerModel(models.Model):
    created_on = models.DateTimeField(auto_now_add=True)
    updated_on = models.DateTimeField(auto_now=True)

    def __iter__(self):
        for field_name in self._meta.get_fields():
            value = getattr(self, field_name, None)
            yield (field_name, value)

    class Meta:
        abstract = True


class Image(OpenLedgerModel):
    identifier = models.UUIDField(
        unique=True,
        db_index=True,
        help_text="Our unique identifier for a CC work."
    )

    source = models.CharField(
        max_length=80,
        blank=True,
        null=True,
        db_index=True,
        help_text="The source of the data, e.g. Google Open Images "
                  "dataset, Flickr, 500px..."
    )

    foreign_identifier = models.CharField(
        unique=True,
        max_length=1000,
        blank=True,
        null=True,
        db_index=True,
        help_text="The identifier provided by the upstream source."
    )

    foreign_landing_url = models.CharField(
        max_length=1000,
        blank=True,
        null=True,
        help_text="The landing page of the work."
    )

    url = models.URLField(
        unique=True,
        max_length=1000,
        help_text="The actual URL to the image."
    )

    thumbnail = models.URLField(
        max_length=1000,
        blank=True,
        null=True,
        help_text="The thumbnail for the image, if any."
    )

    width = models.IntegerField(blank=True, null=True)
    height = models.IntegerField(blank=True, null=True)

    filesize = models.IntegerField(blank=True, null=True)

    license = models.CharField(max_length=50)

    license_version = models.CharField(max_length=25, blank=True, null=True)

    creator = models.CharField(max_length=2000, blank=True, null=True)

    creator_url = models.URLField(max_length=2000, blank=True, null=True)

    title = models.CharField(max_length=2000, blank=True, null=True)

    tags = JSONField(blank=True, null=True)

    tags_list = ArrayField(
        models.CharField(max_length=255), blank=True, null=True
    )

    last_synced_with_source = models.DateTimeField(
        blank=True,
        null=True,
        db_index=True
    )

    removed_from_source = models.BooleanField(default=False)

    meta_data = JSONField(blank=True, null=True)

    view_count = models.IntegerField(default=0)

    watermarked = models.NullBooleanField(blank=True, null=True)

    @property
    def license_url(self):
        _license = str(self.license)
        license_version = str(self.license_version)
        return get_license_url(_license, license_version)

    @property
    def attribution(self):
        _license = str(self.license)
        license_version = str(self.license_version)
        if self.title:
            title = '"' + str(self.title) + '"'
        else:
            title = 'This work'
        if self.creator:
            creator = 'by ' + str(self.creator) + ' '
        else:
            creator = ''
        attribution = ATTRIBUTION.format(
            title=title,
            creator=creator,
            _license=_license.upper(),
            version=license_version,
            license_url=str(self.license_url)
        )
        return attribution

    def image_tag(self):
        return mark_safe('<img src="%s" width="150" />' % self.url)

    image_tag.short_description = 'Image'

    class Meta:
        db_table = 'image'
        ordering = ['-created_on']


class DeletedImages(OpenLedgerModel):
    deleted_id = models.UUIDField(
        unique=True,
        db_index=True,
        help_text="The identifier of the deleted image."
    )
    deleting_user = models.CharField(
        max_length=50,
        help_text="The user that deleted the image."
    )


class ContentSource(models.Model):
    source_identifier = models.CharField(max_length=50)
    source_name = models.CharField(max_length=250, unique=True)
    created_on = models.DateTimeField(auto_now=False)
    domain_name = models.CharField(max_length=500)
    filter_content = models.BooleanField(null=False, default=False)
    notes = models.TextField(null=True)

    class Meta:
        db_table = 'content_source'


class ImageTags(OpenLedgerModel):
    tag = models.ForeignKey(
        'Tag',
        on_delete=models.CASCADE,
        blank=True,
        null=True
    )
    image = models.ForeignKey(Image, on_delete=models.CASCADE, null=True)

    class Meta:
        unique_together = ('tag', 'image')
        db_table = 'image_tags'


class ImageList(OpenLedgerModel):
    title = models.CharField(max_length=2000, help_text="Display name")
    images = models.ManyToManyField(
        Image,
        related_name="lists",
        help_text="A list of identifier keys corresponding to images."
    )
    slug = models.CharField(
        max_length=200,
        help_text="A unique identifier used to make a friendly URL for "
                  "downstream API consumers.",
        unique=True,
        db_index=True
    )
    auth = models.CharField(
        max_length=64,
        help_text="A randomly generated string assigned upon list creation. "
                  "Used to authenticate updates and deletions."
    )

    class Meta:
        db_table = 'imagelist'

    def save(self, *args, **kwargs):
        self.slug = uuslug(self.title, instance=self)
        super(ImageList, self).save(*args, **kwargs)


class Tag(OpenLedgerModel):
    foreign_identifier = models.CharField(max_length=255, blank=True, null=True)
    name = models.CharField(max_length=1000, blank=True, null=True)
    # Source can be a provider/source (like 'openimages', or 'user')
    source = models.CharField(max_length=255, blank=True, null=True)
    slug = models.SlugField(blank=True, null=True, max_length=255)

    class Meta:
        db_table = 'tag'


class ShortenedLink(OpenLedgerModel):
    shortened_path = models.CharField(
        unique=True,
        max_length=10,
        help_text="The path to the shortened URL, e.g. tc3n834. The resulting "
                  "URL will be shares.cc/tc3n834.",
        db_index=True
    )
    full_url = models.URLField(unique=True, max_length=1000, db_index=True)
    created_on = models.DateTimeField(auto_now_add=True, db_index=True)


class OAuth2Registration(models.Model):
    """
    Information about API key applicants.
    """
    name = models.CharField(
        max_length=150,
        unique=True,
        help_text="A unique human-readable name for your application or "
                  "project requiring access to the CC Catalog API."
    )
    description = models.CharField(
        max_length=10000,
        help_text="A description of what you are trying to achieve with your "
                  "project using the API. Please provide as much detail as "
                  "possible!"
    )
    email = models.EmailField(
        help_text="A valid email that we can reach you at if we have any "
                  "questions about your use case or data consumption."
    )


class ThrottledApplication(AbstractApplication):
    """
    An OAuth2 application with adjustable rate limits.
    """
    RATE_LIMIT_MODELS = [
        ('standard', 'standard'),  # Default rate limit for all API keys.
        ('enhanced', 'enhanced')   # Rate limits for "super" keys, granted on a
                                   # case-by-case basis.
    ]
    rate_limit_model = models.CharField(
        max_length=20,
        choices=RATE_LIMIT_MODELS,
        default='standard'
    )
    verified = models.BooleanField(default=False)


class OAuth2Verification(models.Model):
    """
    An email verification code sent by noreply-cccatalog. After verification
    occurs, the entry should be deleted.
    """
    associated_application = models.ForeignKey(
        ThrottledApplication,
        on_delete=models.CASCADE
    )
    email = models.EmailField()
    code = models.CharField(max_length=256, db_index=True)
