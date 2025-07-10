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

def generate_fake_document(providers, **custom_lists):
    generic = providers['generic']
    random_mimesis = providers['random']

    first_name = generic.person.first_name()
    last_name = generic.person.last_name()
    city = random.choice(list(GEOGRAPHIC_CLUSTERS.keys()))

    # Driver Document
    document = {
        "dog_driver_id": f"DD{generic.numeric_string.generate(length=4)}",
        "dog_name": random_mimesis.choice(custom_lists['dog_names']),
        "dog_breed": random_mimesis.choice(custom_lists['dog_breeds']),
        "license_number": f"{random_mimesis.choice(custom_lists['license_plates'])}{generic.numeric_string.generate(length=4)}",
        "favorite_treats": random_mimesis.choice(custom_lists['treats']),
        "preferred_tip": random_mimesis.choice(custom_lists['tips']),
        "vehicle_type": random_mimesis.choice(custom_lists['vehicle_types']),
        "vehicle_make": random_mimesis.choice(custom_lists['vehicle_makes']),
        "vehicle_model": random_mimesis.choice(custom_lists['vehicle_models']),
        "vehicle_year": random_mimesis.choice(custom_lists['vehicle_years']),
        "vehicle_color": random_mimesis.choice(custom_lists['vehicle_colors']),
        "license_plate": random_mimesis.choice(custom_lists['license_plates']),
        "current_location": generate_location(city),
        "status": random.choice(['available', 'busy', 'offline']),
        "current_ride": f"R{generic.numeric_string.generate(length=6)}",
        "account_status": random_mimesis.choice(custom_lists['account_status']),
        "join_date": generic.datetime.formatted_date(),
        "total_rides": generic.numeric.integer_number(start=1, end=200),
        "rating": generic.numeric.float_number(start=1.0, end=5.0, precision=2),
        "earnings": {
            "today": {
                "amount": generic.numeric.float_number(start=1.0, end=5.0, precision=2),
                "currency": "USD"
            },
            "this_week": {
                "amount": generic.numeric.float_number(start=1.0, end=5.0, precision=2),
                "currency": "USD"
            },
            "this_month": {
                "amount": generic.numeric.float_number(start=1.0, end=5.0, precision=2),
                "currency": "USD"
            }
        },
        "last_grooming_check": "2023-12-01",
        "owner": {
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name}{last_name}@gmail.com"
        },
        "special_skills": generic.multiple_choices.generate(custom_lists['skills'], num_of_choices=3),
        "bark_volume": generic.numeric.float_number(start=1.0, end=10.0, precision=2),
        "tail_wag_speed": generic.numeric.float_number(start=1.0, end=10.0, precision=1)
    }

    return document
