import random
import time

import requests
from dagster import ConfigurableResource, InitResourceContext, EnvVar

# Gateway errors from the geocoder are usually momentary; retry them this many times
GEOCODER_RETRY_STATUSES = {502, 503, 504}
GEOCODER_MAX_RETRIES = 2


class GeocodingError(Exception):
    """Custom exception for errors during geocoding operations."""
    pass

class GeocoderResource(ConfigurableResource):
    """A Dagster resource for interacting with the Hack Club Geocoder API."""

    api_key: str = EnvVar("HACKCLUB_GEOCODER_API_KEY")
    base_url: str = "https://geocoder.hackclub.com/v1"

    def geocode(self, address: str) -> dict:
        """Geocode an address using Hack Club Geocoder API.

        Returns:
            dict: Geocoding result with lat, lng, formatted_address, country_name, country_code
        
        Raises:
            GeocodingError: If any error occurs during the geocoding API call.
        """
        try:
            for attempt in range(GEOCODER_MAX_RETRIES + 1):
                response = requests.get(
                    f"{self.base_url}/geocode",
                    params={"address": address, "key": self.api_key},
                    timeout=30
                )
                if response.status_code not in GEOCODER_RETRY_STATUSES or attempt == GEOCODER_MAX_RETRIES:
                    break
                time.sleep(2 * (2 ** attempt) + random.uniform(0, 1))
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise GeocodingError(f"Hack Club Geocoder API failed for address '{address}': {e}") from e
        except Exception as e:
            raise GeocodingError(f"An unexpected error occurred during geocoding for address '{address}': {e}") from e

    def reverse_geocode(self, latlng: tuple[float, float] | dict[str, float]) -> dict:
        """Reverse geocode a latitude/longitude coordinate.
        
        Note: This method is not supported by the Hack Club Geocoder API.
        
        Raises:
            GeocodingError: Always raises as reverse geocoding is not supported.
        """
        raise GeocodingError("Reverse geocoding is not supported by the Hack Club Geocoder API")
