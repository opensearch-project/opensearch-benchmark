from mimesis.providers.base import BaseProvider
from mimesis.enums import TimestampFormat

import random

GEOGRAPHIC_CLUSTERS = {
    'Manhattan': {
        'center': {'lat': 40.7831, 'lon': -73.9712},
        'radius': 0.05  # degrees
    },
    'Brooklyn': {
        'center': {'lat': 40.6782, 'lon': -73.9442},
        'radius': 0.05
    },
    'Austin': {
        'center': {'lat': 30.2672, 'lon': -97.7431},
        'radius': 0.1  # Increased radius to cover more of Austin
    }
}

def generate_location(cluster):
    """Generate a random location within a cluster"""
    center = GEOGRAPHIC_CLUSTERS[cluster]['center']
    radius = GEOGRAPHIC_CLUSTERS[cluster]['radius']
    lat = center['lat'] + random.uniform(-radius, radius)
    lon = center['lon'] + random.uniform(-radius, radius)
    return {'lat': lat, 'lon': lon}

class NumericString(BaseProvider):
    class Meta:
        name = "numeric_string"

    @staticmethod
    def generate(length=5) -> str:
        return ''.join([str(random.randint(0, 9)) for _ in range(length)])

class MultipleChoices(BaseProvider):
    class Meta:
        name = "multiple_choices"

    @staticmethod
    def generate(choices, num_of_choices=5) -> str:
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Choices: %s", choices)
        logger.info("Length: %s", num_of_choices)
        total_choices_available = len(choices) - 1

        return [choices[random.randint(0, total_choices_available)] for _ in range(num_of_choices)]

