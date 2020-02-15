from enum import Enum, auto
from elasticsearch_dsl import Integer, DocType, Field
from ingestion_server.categorize import get_categories

"""
Provides an ORM-like experience for accessing data in Elasticsearch.

Note the actual schema for Elasticsearch is defined in es_mapping.py; any
low-level changes to the index must be represented there as well.
"""


class RankFeature(Field):
    name = 'rank_feature'


class SyncableDocType(DocType):
    """
    Represents tables in the source-of-truth that will be replicated to
    Elasticsearch.
    """
    # Aggregations can't be performed on the _id meta-column, which necessitates
    # copying it to this column in the doc. Aggregation is used to find the last
    # document inserted into Elasticsearch
    id = Integer()

    @staticmethod
    def database_row_to_elasticsearch_doc(row, schema):
        """
        Children of this class must have a function mapping a Postgres model
        to an Elasticsearch document.

        :param row: A tuple representing a row in Postgres.
        :param schema: A map of each field name to its position in the row.
        :return:
        """
        raise NotImplemented(
            'Model is missing database -> Elasticsearch translation.'
        )


class Image(SyncableDocType):
    """
    Represents an image in Elasticsearch. Note that actual mappings are defined
    in `ingestion_server.es_mapping`.
    """
    class AspectRatios(Enum):
        TALL = auto()
        WIDE = auto()
        SQUARE = auto()

    class ImageSizes(Enum):
        """
        Maximum threshold for each image size band
        """
        SMALL = 640 * 480
        MEDIUM = 1600 * 900
        LARGE = float("inf")

    class Index:
        name = 'image'

    @staticmethod
    def database_row_to_elasticsearch_doc(row, schema):
        views, comments, likes = None, None, None
        try:
            metrics = row[schema['meta_data']]['popularity_metrics']
            views = int(metrics['views']) + 1
            likes = int(metrics['likes']) + 1
            comments = int(metrics['comments']) + 1
        except (KeyError, TypeError):
            pass
        source = row[schema['source']]
        extension = Image.get_extension(row[schema['url']])
        height = row[schema['height']]
        width = row[schema['width']]
        return Image(
            _id=row[schema['id']],
            id=row[schema['id']],
            title=row[schema['title']],
            identifier=row[schema['identifier']],
            creator=row[schema['creator']],
            creator_url=row[schema['creator_url']],
            tags=Image.parse_detailed_tags(row[schema['tags']]),
            created_on=row[schema['created_on']],
            url=row[schema['url']],
            thumbnail=row[schema['thumbnail']],
            source=row[schema['source']],
            license=row[schema['license']].lower(),
            license_version=row[schema['license_version']],
            foreign_landing_url=row[schema['foreign_landing_url']],
            view_count=row[schema['view_count']],
            description=Image.parse_description(row[schema['meta_data']]),
            height=height,
            width=width,
            extension=Image.get_extension(row[schema['url']]),
            views=views,
            comments=comments,
            likes=likes,
            categories=get_categories(extension, source),
            aspect_ratio=Image.get_aspect_ratio(height, width),
            size=Image.get_size(height, width),
            license_url=Image.get_license_url(row[schema['meta_data']])
        )

    @staticmethod
    def parse_description(metadata_field):
        """
        Parse the description field from the metadata if available.

        Limit to the first 2000 characters.
        """
        try:
            if 'description' in metadata_field:
                return metadata_field['description'][:2000]
        except TypeError:
            return None

    @staticmethod
    def get_extension(url):
        extension = url.split('.')[-1].lower()
        if '/' in extension or extension is None:
            return None
        else:
            return extension

    @staticmethod
    def get_aspect_ratio(height, width):
        if height is None or width is None:
            return None
        elif height > width:
            aspect_ratio = Image.AspectRatios.TALL.name
        elif height < width:
            aspect_ratio = Image.AspectRatios.WIDE.name
        else:
            aspect_ratio = Image.AspectRatios.SQUARE.name
        return aspect_ratio.lower()

    @staticmethod
    def get_size(height, width):
        if height is None or width is None:
            return None
        resolution = height * width
        for size in Image.ImageSizes:
            if resolution < size.value:
                return size.name.lower()

    @staticmethod
    def get_license_url(meta_data):
        """
        If the license_url is not provided, we'll try to generate it elsewhere
        from the `license` and `license_version`.
        """
        if 'license_url' in meta_data:
            return meta_data['license_url']
        else:
            return None

    @staticmethod
    def parse_detailed_tags(json_tags):
        if json_tags:
            parsed_tags = []
            for tag in json_tags:
                if 'name' in tag:
                    parsed_tag = {'name': tag['name']}
                    if 'accuracy' in tag:
                        parsed_tag['accuracy'] = tag['accuracy']
                    parsed_tags.append(parsed_tag)
            return parsed_tags
        else:
            return None


# Table name -> Elasticsearch model
database_table_to_elasticsearch_model = {
    'image': Image
}
