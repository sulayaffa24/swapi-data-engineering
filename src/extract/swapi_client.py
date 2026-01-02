import requests
import pandas as pd 

# Simple cache for nested URLs to avoid repeated HTTP calls
URL_CACHE = {}

def get_data(url):
    """
    This function fetches JSON data from the SWAPI
    website. 
    """
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def get_all_pages(url):
    """
    Handles pagination - returns all `results` across pages
    from the SWAPI
    """
    results = []

    while url:
        data = get_data(url)
        
        if isinstance(data, dict) and "results" in data:
            results.extend(data["results"])
            url = data.get("next")

        elif isinstance(data, list):
            results.extend(data)
            url = None

        elif isinstance(data, dict):
            results.append(data)
            url = None

        else:
            raise TypeError(f"Unexpected response type from API {type(data)}")

    return results

def _fetch_name_or_title(url):
    """
    Fetches a URL and returns 'name' or 'title', using a cache
    """
    if url in URL_CACHE:
        return URL_CACHE[url]

    try:
        sub_data = get_data(url)
        value = sub_data.get("name") or sub_data.get("title")
    except Exception:
        value = None

    URL_CACHE[url] = value
    
    return value

def nested_urls(value):
    """
    Handles nested API URLs
    - If value is a list of URLs, fetch names or titles.
    - If single URL, fetch and return name or title.
    - Otherwise, return as is
    """

    # List of URLs
    if isinstance(value, list) and all(isinstance(v, str) and v.startswith("http") for v in value):
        names = []
        for v in value:
            names.append(_fetch_name_or_title(v))
        return names

    # Single URL
    if isinstance(value, str) and value.startswith("http"):
        return _fetch_name_or_title(value)
    
    # Non-URL value 
    return value


def normalize_entity_data(entity_data):
    """
    Replaces nested URLs in an entity's attributes with human-readble
    names/titles
    """
    normalized = []

    for item in entity_data:
        record = {}
        for key, value in item.items():
            record[key] = nested_urls(value)
        normalized.append(record)
    return normalized

# URL Usage
def get_normalized_planets(url: str = "https://swapi.info/api/planets"):
    """
    Convenience function:
    - fetch all planets
    - normalize nested URLs
    - return as a Pandas DataFrame
    """
    raw_planets = get_all_pages(url)
    normalized_planets = normalize_entity_data(raw_planets)
    return pd.DataFrame(normalized_planets)

if __name__ == "__main__":
    df_planets =  get_normalized_planets()
    print(df_planets.head())