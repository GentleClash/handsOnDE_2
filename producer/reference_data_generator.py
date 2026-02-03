import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from config_constants import *

class ReferenceDataGenerator:
    """Generates reference CSV data for restaurants, riders, zones, and pricing"""
    
    def __init__(self, output_dir: str = 'data/reference', corrupted_row_rate: float = 0.005, override: bool = False) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Error injection rates for reference data
        self.corrupted_row_rate = corrupted_row_rate 
        self.override = override
    
    def generate_zones(self, regions : Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Generate zones.csv data"""
        zones = []
        zone_id = 1

        if not regions:
            regions = REGIONS
        
        for region in regions:
            for _ in range(ZONES_PER_REGION):
                # Generate simple rectangular boundary
                lat_center = random.uniform(GPS_BOUNDS['lat_min'], GPS_BOUNDS['lat_max'])
                lon_center = random.uniform(GPS_BOUNDS['lon_min'], GPS_BOUNDS['lon_max'])
                boundary = self._generate_boundary_polygon(lat_center, lon_center)
                
                zones.append({
                    'zone_id': f'Z{zone_id:04d}',
                    'city': f'City_{region.title()}',
                    'region': region,
                    'boundary_polygon': json.dumps(boundary)
                })
                zone_id += 1
        
        return zones
    
    def _generate_boundary_polygon(self, lat: float, lon: float, size: float = 0.02) -> List[List[float]]:
        """Generate a simple rectangular boundary polygon"""
        return [
            [lat - size, lon - size],
            [lat - size, lon + size],
            [lat + size, lon + size],
            [lat + size, lon - size],
            [lat - size, lon - size]  
        ]
    
    def generate_restaurants(self, zones: List[Dict[str, Any]], num_restaurants: int = 500) -> List[Dict[str, Any]]:
        """Generate restaurants.csv data"""
        restaurants = []
        
        for i in range(num_restaurants):
            zone = random.choice(zones)
            restaurants.append({
                'restaurant_id': f'R{i+1:06d}',
                'name': f'Restaurant_{i+1}',
                'zone_id': zone['zone_id'],
                'region': zone['region'],
                'cuisine_type': random.choice(CUISINE_TYPES),
                'active_flag': random.choices([True, False], weights=[0.9, 0.1])[0]
            })
        
        return restaurants
    
    def generate_riders(self, zones: List[Dict[str, Any]], num_riders: int = 1000) -> List[Dict[str, Any]]:
        """Generate riders.csv data"""
        riders = []
        
        for i in range(num_riders):
            zone = random.choice(zones)
            riders.append({
                'rider_id': f'RD{i+1:06d}',
                'name': f'Rider_{i+1}',
                'phone': f'+91{random.randint(7000000000, 9999999999)}',
                'zone_id': zone['zone_id'],
                'rating': round(random.uniform(1.2, 5.0), 2),
                'total_deliveries': random.randint(50, 100) if random.random() < 0.8 else int(random.gauss(100, 50)),
                'has_left' : False
                
            })
        
        return riders
    
    def generate_single_rider(self, zones: List[Dict[str, Any]], rider_number: int) -> Dict[str, Any]:
        """Generate a single new rider"""
        zone = random.choice(zones)
        return {
            'rider_id': f'RD{rider_number:06d}',
            'name': f'Rider_{rider_number}',
            'phone': f'+91{random.randint(7000000000, 9999999999)}',
            'zone_id': zone['zone_id'],
            'rating': round(random.uniform(3.5, 5.0), 2),  # New riders start with decent rating
            'total_deliveries': 0,  # New rider, no deliveries yet
            'has_left': False
        }
    
    def generate_pricing_rules(self, zones: List[Dict]) -> List[Dict[str, Any]]:
        """Generate pricing_rules.csv data"""
        rules = []
        rule_id = 1
        
        for zone in zones:
            # Each zone has 1-3 pricing rules
            num_rules = random.randint(1, 3)
            
            for _ in range(num_rules):
                valid_from = datetime.now() - timedelta(days=random.randint(30, 365))
                valid_to = valid_from + timedelta(days=random.randint(90, 365))
                
                rules.append({
                    'rule_id': f'PR{rule_id:05d}',
                    'zone_id': zone['zone_id'],
                    'base_fee': round(random.uniform(20, 50), 2),
                    'per_km_rate': round(random.uniform(5, 15), 2),
                    'valid_from': valid_from.strftime('%Y-%m-%d'),
                    'valid_to': valid_to.strftime('%Y-%m-%d')
                })
                rule_id += 1
        
        return rules
    
    def _inject_corrupted_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Corrupt a row by removing or mangling data"""
        if random.random() < self.corrupted_row_rate:
            keys = list(row.keys())
            key_to_corrupt = random.choice(keys)
            row[key_to_corrupt] = '###CORRUPTED###'
        return row
    
    def read_csv(self, filename: str) -> List[Dict]:
        """Read data from CSV file"""
        filepath = self.output_dir / filename
        data = []
        
        if not filepath.exists():
            return data
        
        with open(filepath, 'r', newline='') as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
        
        return data

    def write_csv(self, data: List[Dict], filename: str, update_riders: bool = False) -> str:
        """Write data to CSV with optional error injection"""
        filepath = self.output_dir / filename
        
        if not data:
            return str(filepath)
        
        if not self.override and filepath.exists() and not update_riders:
            print(f"{filename} already exists and override is set to False. Skipping write.")
            return str(filepath)
        
        # Apply error injection
        processed_data = []
        for row in data:
            row = self._inject_corrupted_row(row)
            processed_data.append(row)
        
        # Get base fieldnames from first non-drifted row, then add extra columns at the end
        base_keys = [k for k in data[0].keys() if not k.startswith('_extra')]
        extra_keys = sorted(set(
            k for row in processed_data 
            for k in row.keys() 
            if k.startswith('_extra')
        ))
        fieldnames = base_keys + extra_keys
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_data)
        
        return str(filepath)
    
    def generate_all(self) -> Dict[str, str]:
        """Generate all reference data files"""

        if not self.override and all((self.output_dir / fname).exists() for fname in ['zones.csv', 'restaurants.csv', 'riders.csv', 'pricing_rules.csv']):
            print("Reference data files already exist and override is set to False. Skipping generation.")
            return {
                'zones': str(self.output_dir / 'zones.csv'),
                'restaurants': str(self.output_dir / 'restaurants.csv'),
                'riders': str(self.output_dir / 'riders.csv'),
                'pricing_rules': str(self.output_dir / 'pricing_rules.csv')
            }
        
        if (self.output_dir / 'zones.csv').exists() and not self.override:
            zones = self.read_csv('zones.csv')
        else:
            zones = self.generate_zones()

        if (self.output_dir / 'restaurants.csv').exists() and not self.override:
            restaurants = self.read_csv('restaurants.csv')
        else:
            restaurants = self.generate_restaurants(zones)

        if (self.output_dir / 'riders.csv').exists() and not self.override:
            riders = self.read_csv('riders.csv')
        else:
            riders = self.generate_riders(zones)

        if (self.output_dir / 'pricing_rules.csv').exists() and not self.override:
            pricing_rules = self.read_csv('pricing_rules.csv')
        else:
            pricing_rules = self.generate_pricing_rules(zones)
        
        return {
            'zones': self.write_csv(zones, 'zones.csv'),
            'restaurants': self.write_csv(restaurants, 'restaurants.csv'),
            'riders': self.write_csv(riders, 'riders.csv'),
            'pricing_rules': self.write_csv(pricing_rules, 'pricing_rules.csv')
        }

if __name__ == "__main__":
    pass
    