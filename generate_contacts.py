import csv
import random

# Set seed for reproducibility (optional)
random.seed(42)

# Sample data pools
first_names = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa"
]
last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
]
cities = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle",
    "Denver", "Washington", "Boston", "Nashville", "Baltimore", "Oklahoma City"
]
countries = [
    "USA", "Canada", "UK", "Australia", "Germany", "France", "Spain", "Italy",
    "Japan", "Brazil", "Mexico", "India", "China", "South Africa", "Russia"
]
streets = [
    "Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Lake",
    "Hill", "Park", "View", "River", "Spring", "Church", "Market"
]

# Create CSV file
with open('contacts.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Write header
    writer.writerow(["First Name", "Last Name", "Email", "Phone", "Address", "City", "Country"])

    for i in range(1, 501):
        first = random.choice(first_names)
        last = random.choice(last_names)
        # Email: first.last + random number @example.com
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@example.com"
        # Phone: three groups of digits
        phone = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        # Address: number + street name + "St" (or "Ave" etc.)
        address = f"{random.randint(1, 999)} {random.choice(streets)} St"
        city = random.choice(cities)
        country = random.choice(countries)

        writer.writerow([first, last, email, phone, address, city, country])

print("✅ contacts.csv with 500 contacts has been created.")