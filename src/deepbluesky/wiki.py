# wiki.py
# wiki lookup logic here
from __future__ import annotations

import re

from typing import Optional
from typing import List, Tuple

import requests

def relative_to_absolute_location(location: str, query_url: str) -> str:
    query_url = re.sub(r'\?.*$', '', query_url)
    if location.startswith('/'):
        server = re.sub(r'^([a-zA-Z]+://[^/]*)/.*$', r'\1', query_url)
        return server + location
    if re.match(r'^[a-zA-Z]+://', location):
        return location
    return re.sub(r'^(([^/]*/)+)[^/]*', r'\1', query_url) + '/' + location

def lookup_tvtropes(article: str) -> Tuple[bool, str]:
    parts = re.sub(r'[^\w/]', '', article).split('/', maxsplit=1)
    if len(parts) > 1:
        namespace = parts[0]
        title = parts[1]
    else:
        namespace = 'Main'
        title = parts[0]
    server = 'https://tvtropes.org'
    query = '/pmwiki/pmwiki.php/' + namespace + '/' + title
    result = requests.get(server + query, allow_redirects=False)
    if 'location' in result.headers:
        location = relative_to_absolute_location(result.headers['location'], server + query)
        return (True, location)
    result.encoding = 'UTF-8'
    if re.search(r"<div>Inexact title\. See the list below\. We don't have an article named <b>{}</b>/{}, exactly\. We do have:".format(namespace, title), result.text, flags=re.IGNORECASE):
        return (False, result.url)
    return (True, result.url) if result.ok else (False, '')

def lookup_mediawiki(mediawiki_base: str, article: str) -> Optional[str]:
    parts = article.split('/')
    parts = [re.sub(r'\s+', r'_', part).strip('_') for part in parts]
    article = '/'.join(parts)
    params = {
        'title': 'Special:Search',
        'go': 'Go',
        'ns0': '1',
        'search': article,
    }
    result = requests.head(mediawiki_base, params=params)
    if 'location' in result.headers:
        location = relative_to_absolute_location(result.headers['location'], mediawiki_base)
        if ':' in location[7:]:
            # Location is a user page
            second_result = requests.head(location)
            # If the user exists but they have no user page, then mediawiki will return 200
            # But the last-modified header only is preset if the user page also exists
            return location if second_result.ok and 'last-modified' in second_result.headers else None
        return location
    return None

def lookup_wikis(article: str, extra_wikis: List[str]) -> str:
    for wiki in extra_wikis:
        wiki_url = lookup_mediawiki(wiki, article)
        if wiki_url:
            return wiki_url
    success, tv_url = lookup_tvtropes(article.strip())
    if success:
        return tv_url
    wiki_url = lookup_mediawiki('https://en.wikipedia.org/w/index.php', article)
    if wiki_url:
        return wiki_url
    return f'Inexact Title Disambiguation Page Found:\n{tv_url}' if tv_url else f'Unable to locate article: `{article}`'
