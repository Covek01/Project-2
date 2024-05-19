import random

class Airplane:
    def __init__(self, identifier):
        self.identifier = identifier
        self.callsign = f"AB{random.randint(1000, 9999)}"
        self.country = random.choice(["USA", "Germany", "France", "UK", "Japan"])
        self.latitude = random.uniform(-75.51353, 75.75)
        self.on_ground = random.choice([True, False])
        self.geo_altitude = random.uniform(1000, 12000)  # altitude in meters
        self.velocity = random.uniform(200, 1000)  # velocity in m/s
        self.air_density = random.uniform(0.8, 1.2)  # air density in kg/m^3
        self.update_counter = 0

    def update(self):
        # Update attributes with small random variations
        self.latitude += random.uniform(-0.01, 0.01)
        self.geo_altitude += random.uniform(-100, 100)
        self.velocity += random.uniform(-5, 5)
        self.air_density += random.uniform(-0.01, 0.01)
        # Ensure values stay within realistic bounds
        self.latitude = min(max(self.latitude, -75.51353), 75.75)
        self.geo_altitude = min(max(self.geo_altitude, 0), 12000)
        self.velocity = min(max(self.velocity, 0), 1000)
        self.air_density = min(max(self.air_density, 0.1), 2.0)

        self.update_counter += 1
        if self.update_counter >= 20:  # 5% of the time (1 in 20 updates)
            self.on_ground = not self.on_ground
            self.update_counter = 0


    def to_dict(self):
        return {
            "callsign": self.callsign,
            "country": self.country,
            "latitude": self.latitude,
            "on_ground": self.on_ground,
            "geo_altitude": self.geo_altitude,
            "velocity": self.velocity,
            "air_density": self.air_density
        }
