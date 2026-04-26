"""
generate_sample_data.py
Generates a realistic sample NYC Taxi dataset for pipeline testing.
"""
import csv
import random
from datetime import datetime, timedelta

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
PAYMENT_TYPES = ["Credit Card", "Cash", "No Charge", "Dispute"]

def random_datetime(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def generate_sample(n=1000, output_path="data/sample_taxi.csv"):
    start = datetime(2023, 1, 1)
    end = datetime(2023, 12, 31)

    rows = []
    for i in range(n):
        pickup_dt = random_datetime(start, end)
        trip_minutes = random.randint(3, 90)
        dropoff_dt = pickup_dt + timedelta(minutes=trip_minutes)
        distance = round(random.uniform(0.5, 25.0), 2)
        fare = round(2.5 + distance * 2.5 + random.uniform(0, 5), 2)
        tip = round(fare * random.uniform(0, 0.3), 2)
        total = round(fare + tip + 0.5, 2)

        rows.append({
            "trip_id": f"TRIP_{i+1:06d}",
            "pickup_datetime": pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "dropoff_datetime": dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "pickup_borough": random.choice(BOROUGHS),
            "dropoff_borough": random.choice(BOROUGHS),
            "passenger_count": random.randint(1, 6),
            "trip_distance_miles": distance,
            "fare_amount": fare,
            "tip_amount": tip,
            "total_amount": total,
            "payment_type": random.choice(PAYMENT_TYPES),
            "trip_year": pickup_dt.year,
            "trip_month": pickup_dt.month
        })

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Generated {n} sample records -> {output_path}")

if __name__ == "__main__":
    generate_sample()
