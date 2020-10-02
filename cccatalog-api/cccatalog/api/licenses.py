import rdflib
from django.core.cache import cache

LICENSES = (
    ("BY", "Attribution"),
    ("BY-NC", "Attribution NonCommercial"),
    ("BY-ND", "Attribution NoDerivatives"),
    ("BY-SA", "Attribution ShareAlike"),
    ("BY-NC-ND", "Attribution NonCommercial NoDerivatives"),
    ("BY-NC-SA", "Attribution NonCommercial ShareAlike"),
    ("PDM", "Public Domain Mark"),
    ("CC0", "Public Domain Dedication"),
)

LICENSE_GROUPS = {
    # All open licenses
    "all": {'BY', 'BY-NC', 'BY-ND', 'BY-SA', 'BY-NC-ND', 'BY-NC-SA', 'PDM',
            'CC0'},
    # All CC licenses
    "all-cc": {'BY', 'BY-NC', 'BY-ND', 'BY-SA', 'BY-NC-ND', 'BY-NC-SA', 'CC0'},
    # All licenses allowing commercial use
    "commercial": {'BY', 'BY-SA', 'BY-ND', 'CC0', 'PDM'},
    # All licenses allowing modifications
    "modification": {'BY', 'BY-SA', 'BY-NC', 'BY-NC-SA', 'CC0', 'PDM'},
}

ATTRIBUTION = \
    "{title} {creator}is licensed under CC-{_license} {version}. To view a " \
    "copy of this license, visit {license_url}."


def get_license_url(_license, version, meta_data=None):
    license_overridden = meta_data and 'license_url' in meta_data
    if license_overridden and meta_data['license_url'] is not None:
        return meta_data['license_url']
    elif _license.lower() == 'pdm':
        return 'https://creativecommons.org/publicdomain/mark/1.0/'
    else:
        return f'https://creativecommons.org/licenses/{_license}/{version}/'


def parse_rdf_cache_license(rdf_path):
    g = rdflib.Graph()
    result = g.parse(rdf_path)
    licenses = []
    for s, p, o in g:
        license_version = ""
        license_url = ""
        jurisdiction = ""
        language_code = ""
        s = s.split("/")
        license_url = "/".join(s[0:6])
        license_version = s[5]
        if len(s) >= 8:
            jurisdiction = s[-2]
        if type(o) == rdflib.Literal:
            language_code = o.language
        licenses.append({
            'license_url': license_url,
            'license_version': license_version,
            'jurisdiction': jurisdiction,
            'language_code': language_code
        })
    if 'licenses' not in cache:
        cache.set('licenses', licenses)
