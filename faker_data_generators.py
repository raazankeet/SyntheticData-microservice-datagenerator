# faker_data_generators.py
from faker import Faker
import random
from faker.providers import BaseProvider

# Initialize Faker instance
fake = Faker()


class CustomPhoneNumberProvider(BaseProvider):
    def custom_phone_number(self):
        area_code = self.random_int(100, 999)
        central_office_code = self.random_int(100, 999)
        station_number = self.random_int(1000, 9999)
        return f"({area_code}){central_office_code}-{station_number}"

fake.add_provider(CustomPhoneNumberProvider)

# Generator functions for each type
def randomNumber():
    """Generates a random integer."""
    return fake.random_int(min=100, max=999999999)

def hospitalName():
    """Generates a fake hospital name."""
    return fake.company()

def hospitalType():
    """Generates a random hospital type."""
    # Define a list of hospital types with more detail
    hospital_types = [
        "General Hospital",
        "Specialized Hospital",
        "Teaching Hospital",
        "Trauma Center",
        "Children's Hospital",
        "Psychiatric Hospital",
        "Rehabilitation Hospital",
        "Cancer Treatment Center",
        "Maternity Hospital"
    ]
    
    # Return a random hospital type from the list
    return fake.random_element(elements=hospital_types)


def addressline1():
    """Generates a fake street address."""
    return fake.street_address()

def addressline2():
    """Generates a fake street address."""
    return fake.secondary_address()

def city():
    """Generates a fake city name."""
    return fake.city()

def state():
    """Generates a fake state name."""
    return fake.state()

def zipcode():
    """Generates a fake zipcode."""
    return fake.zipcode()

def fullAddress():
    return fake.address()

def phoneNumber():
    """Generates a fake phone number."""
    return fake.custom_phone_number()

def emailID():
    """Generates a fake email address."""
    return fake.email()


def bedsCount():
    """Generates a random number for the number of beds in a hospital."""
    return fake.random_int(min=50, max=500)

def boolean():
    """Generates a random boolean value."""
    return fake.boolean()

def firstName():
    """Generates a fake first name."""
    return fake.first_name()

def lastName():
    """Generates a fake last name."""
    return fake.last_name()


def pastDate():
    return fake.date_this_decade(before_today=True).strftime('%Y-%m-%d')  # Date from the past decade

# Generate a future date
def futureDate():
    return fake.future_date().strftime('%Y-%m-%d')   # Date from the next decade

def gender():
    """Generates a random gender."""
    return random.choice(["M", "F","U"])

def specialization():
    """Generates a fake job title or specialization."""
    return fake.job()

def hospitalID(existing_hospital_ids):
    """Generates a hospital ID by picking from existing hospital IDs."""
    return random.choice(existing_hospital_ids)

def ssn():
    """Generates a fake email address."""
    return fake.ssn()

def dollarAmount(min_value=100, max_value=5000000, decimal_places=2):
    # Generate a random float between min_value and max_value, and round to decimal_places
    coverage_amount = round(random.uniform(min_value, max_value), decimal_places)
    return coverage_amount

# Generate gender
def gender():
    return random.choice(["M", "F", "N", "O"])

def claimStatus():
    """Generates a random gender."""
    return random.choice(["Inprogress", "Approved","Rejected"])

def hospitalName():
        
        # List of common hospital suffixes and prefixes
        hospital_prefixes = [
            "Saint", "General", "City", "Central", "Regional", "Community", 
            "Children's", "Veterans", "University", "National", "Memorial", 
            "King", "Queen", "East", "West", "North", "South"
        ]
        hospital_suffixes = [
            "Hospital", "Medical Center", "Health System", "Clinic", 
            "Research Institute", "Care Center", "Specialty Hospital"
        ]
        hospital_types = [
            "Cancer", "Heart", "Neurology", "Orthopedic", "Pediatric", 
            "Surgical", "Maternity", "Psychiatric", "Emergency", "Rehabilitation"
        ]
        
        # Generate a random hospital name
        prefix = random.choice(hospital_prefixes)
        suffix = random.choice(hospital_suffixes)
        hospital_type = random.choice(hospital_types)
        
        # Combine to form a hospital name
        return f"{prefix} {hospital_type} {suffix}"