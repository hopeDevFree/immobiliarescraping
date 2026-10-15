"""The user's exact search URL; no additional filters or ordering."""

import math

VOLLA_CENTER = (40.87414, 14.34105)
RADIUS_METERS = 5000
DEFAULT_SEARCH = "volla-radius"

# Preserve the exact supplied URL, including parameter order and encoding.
USER_SEARCH_URL = "https://www.immobiliare.it/search-list/?idContratto=2&idCategoria=1&arredato=on&__lang=it&raggio=5000&centro=40.87414%2C14.34105&pag=1"
RADIUS_URL = USER_SEARCH_URL

# Municipality searches remain available only for reproducing the earlier probes.
SEARCHES = {
    DEFAULT_SEARCH: RADIUS_URL,
    "volla": "https://www.immobiliare.it/affitto-case/volla/?prezzoMassimo=700",
    "cercola": "https://www.immobiliare.it/affitto-case/cercola/?prezzoMassimo=700",
    "ponticelli": "https://www.immobiliare.it/affitto-case/napoli/ponticelli/?prezzoMassimo=700",
}


def distance_from_volla(location):
    """Great-circle distance to the published pin, never a driving distance."""
    result = {"distance_from_volla_km": None, "location_marker": location.get("marker")}
    try:
        lat = float(location["latitude"])
        lon = float(location["longitude"])
        if not (math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180):
            return result
    except (KeyError, TypeError, ValueError):
        return result
    lat1, lon1 = map(math.radians, VOLLA_CENTER)
    lat2, lon2 = map(math.radians, (lat, lon))
    haversine = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    result["distance_from_volla_km"] = round(
        6371.0088 * 2 * math.asin(math.sqrt(min(1, max(0, haversine)))), 3
    )
    return result
