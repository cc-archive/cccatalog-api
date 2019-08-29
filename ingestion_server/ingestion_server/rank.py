"""
To improve the relevance of search results, we prefer curated collections (such 
as those from the MET museum, NYPL, Rjiksmuseum, ...) over social media sites,
such as Flickr and Behance.

If there are matches from a curated collection, they will appear in the results 
first, even if there is a stronger keyword match in a social collection.
"""
DEPRIORITIZED = 'deprioritized'
DEFAULT = 'default'


relevance_categories = {
    DEFAULT: 10,
    DEPRIORITIZED: 1
}

provider_relevance_category = {
    'behance': DEPRIORITIZED,
    'flickr': DEPRIORITIZED,
    'svgsilh': DEPRIORITIZED,
    'deviantart': DEPRIORITIZED,
    'thingiverse': DEPRIORITIZED
}


def get_provider_relevance(provider):
    if provider in provider_relevance_category:
        relevance_category = provider_relevance_category[provider]
        return relevance_categories[relevance_category]
    else:
        return relevance_categories[DEFAULT]
