"""
Synthetic Data Generator for Delivery Platform Events

Generates realistic operational events (orders, riders) and reference data (CSV)
"""

import uuid
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import csv

from demandmodel import DemandModel
from error_injection import ErrorInjector

random.seed(42)


# =============================================================================
# Configuration Constants
# =============================================================================

REGIONS = ['north', 'south', 'east', 'west', 'central']
ZONES_PER_REGION = 10
PAYMENT_METHODS = ['card', 'upi', 'cash', 'wallet']
CUISINE_TYPES = ['indian', 'chinese', 'italian', 'american', 'mexican', 'thai', 'japanese']
ORDER_EVENT_TYPES = ['order_created', 'order_accepted', 'order_picked_up', 'order_delivered', 'order_cancelled']
RIDER_EVENT_TYPES = ['shift_started', 'shift_ended', 'location_ping', 'order_assigned']
RIDER_STATUSES = ['idle', 'busy', 'offline']

# GPS boundaries
GPS_BOUNDS = {
    'lat_min': 12.85,
    'lat_max': 13.15,
    'lon_min': 77.45,
    'lon_max': 77.75
}

PROBABILITIES = {
    'order_created': 1.0,
    'order_accepted': 0.99,
    'order_assigned': 0.4, # 40% chance of assignment when rider is available
    'order_picked_up': 0.97,
    'order_cancelled': 0.05,
    'order_delivered': 0.95,
    'rider_shift_started': 1.0,
    'rider_shift_ended': 0.05 # riders end shifts in a minute chance

}


# =============================================================================
# Reference Data Generators
# =============================================================================

class ReferenceDataGenerator:
    """Generates reference CSV data for restaurants, riders, zones, and pricing"""
    
    def __init__(self, output_dir: str = 'data/reference', corrupted_row_rate: float = 0.005) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Error injection rates for reference data
        self.corrupted_row_rate = corrupted_row_rate 
    
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
    
    def generate_restaurants(self, zones: List[Dict], num_restaurants: int = 500) -> List[Dict[str, Any]]:
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
    
    def generate_riders(self, zones: List[Dict], num_riders: int = 1000) -> List[Dict[str, Any]]:
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
                'total_deliveries': random.randint(50, 100) if random.random() < 0.8 else int(random.gauss(100, 50))
                
            })
        
        return riders
    
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
    
    def write_csv(self, data: List[Dict], filename: str) -> str:
        """Write data to CSV with optional error injection"""
        filepath = self.output_dir / filename
        
        if not data:
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
        zones = self.generate_zones()
        restaurants = self.generate_restaurants(zones)
        riders = self.generate_riders(zones)
        pricing_rules = self.generate_pricing_rules(zones)
        
        return {
            'zones': self.write_csv(zones, 'zones.csv'),
            'restaurants': self.write_csv(restaurants, 'restaurants.csv'),
            'riders': self.write_csv(riders, 'riders.csv'),
            'pricing_rules': self.write_csv(pricing_rules, 'pricing_rules.csv')
        }


# =============================================================================
# Event Generators
# =============================================================================

class OrderEventGenerator:
    """Generates order lifecycle events"""
    
    def __init__(self, restaurants: List[Dict], riders: List[Dict], zones: List[Dict]) -> None:
        self.restaurants = restaurants
        self.riders = riders
        self.zones = zones
        self.demand_model = DemandModel(base_lambda=50)
        self.error_injector = ErrorInjector()
        self.active_orders: Dict[str, Dict] = {}  # Track order state

        self.rider_delivery_counts: Dict[str, int] = {
            rider['rider_id']: rider['total_deliveries'] for rider in riders
        }
    
    def generate_order_event(
        self, 
        timestamp: datetime,
        event_type: str,
        order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate a single order event"""
        
        if event_type == 'order_created':
            # New order
            order_id = str(uuid.uuid4())
            restaurant = random.choice(self.restaurants)
            rider = random.choice(self.riders)
            
            order_value = min(max(random.lognormvariate(mu=5.5, sigma=0.6), 100), 2000)
            
            # Payment method with realistic distribution: UPI > Cash ≈ Card > Wallet
            payment_method = random.choices(
                PAYMENT_METHODS,
                weights=[0.45, 0.35, 0.15, 0.05]  # upi, cash, card, wallet
            )[0]
            
            event = {
                'event_id': str(uuid.uuid4()),
                'event_timestamp': timestamp.isoformat(),
                'event_type': event_type,
                'order_id': order_id,
                'restaurant_id': restaurant['restaurant_id'],
                'rider_id': None,  # Not assigned yet
                'customer_id': f'C{random.randint(1, 100000):06d}',
                'region': restaurant['region'],
                'zone_id': restaurant['zone_id'],
                'order_value': round(order_value, 2),
                'payment_method': payment_method
            }
            
            # Track order state
            self.active_orders[order_id] = {
                'event': event,
                'created_at': timestamp
            }
            
        elif order_id and order_id in self.active_orders:
            # Existing order state transition
            base_event = self.active_orders[order_id]['event']
            rider = random.choice(self.riders)
            
            event = {
                'event_id': str(uuid.uuid4()),
                'event_timestamp': timestamp.isoformat(),
                'event_type': event_type,
                'order_id': order_id,
                'restaurant_id': base_event['restaurant_id'],
                'rider_id': rider['rider_id'] if event_type != 'order_cancelled' else base_event.get('rider_id'),
                'customer_id': base_event['customer_id'],
                'region': base_event['region'],
                'zone_id': base_event['zone_id'],
                'order_value': base_event['order_value'],
                'payment_method': base_event['payment_method']
            }

            # Increment rider's delivery count when order is delivered
            if event_type == 'order_delivered' and event['rider_id']:
                self.rider_delivery_counts[event['rider_id']] = \
                    self.rider_delivery_counts.get(event['rider_id'], 0) + 1
            
            # Clean up completed/cancelled orders
            if event_type in ['order_delivered', 'order_cancelled']:
                del self.active_orders[order_id]

        else:
            # Fallback for orphaned events
            restaurant = random.choice(self.restaurants)
            order_value = min(max(random.lognormvariate(mu=5.5, sigma=0.6), 100), 2000)
            payment_method = random.choices(
                PAYMENT_METHODS,
                weights=[0.45, 0.35, 0.15, 0.05]  # upi, cash, card, wallet
            )[0]
            event = {
                'event_id': str(uuid.uuid4()),
                'event_timestamp': timestamp.isoformat(),
                'event_type': event_type,
                'order_id': order_id or str(uuid.uuid4()),
                'restaurant_id': restaurant['restaurant_id'],
                'rider_id': random.choice(self.riders)['rider_id'],
                'customer_id': f'C{random.randint(1, 100000):06d}',
                'region': restaurant['region'],
                'zone_id': restaurant['zone_id'],
                'order_value': round(order_value, 2),
                'payment_method': payment_method
            }
        
        return event
    
    def generate_events_for_minute(self, timestamp: datetime) -> List[Dict[str, Any]]:
        """Generate all order events for a given minute"""
        events = []
        
        # Get order count from demand model
        order_count = self.demand_model.generate_order_rate(timestamp)
        
        # Generate new orders
        for _ in range(order_count):
            event = self.generate_order_event(timestamp, 'order_created')
            processed_event = self.error_injector.inject_errors(event)
            
            if processed_event:
                events.append(processed_event)
            
        # Progress existing orders through proper lifecycle with realistic ratios
        # Expected flow: created -> accepted -> picked_up -> delivered (or cancelled at any stage)
        for order_id, order_data in list(self.active_orders.items()):
            created_at = order_data['created_at']
            current_state = order_data.get('state', 'created')
            elapsed = (timestamp - created_at).total_seconds() / 60
            
            # Determine next state based on current state and elapsed time
            next_event = None
            if current_state == 'created' and elapsed > 2:
                
                if random.random() < PROBABILITIES['order_accepted']:
                    next_event = 'order_accepted'
                    self.active_orders[order_id]['state'] = 'accepted'
                else:
                    next_event = 'order_cancelled'
            elif current_state == 'accepted' and elapsed > 8:

                if random.random() < PROBABILITIES['order_picked_up']:
                    next_event = 'order_picked_up'
                    self.active_orders[order_id]['state'] = 'picked_up'
                else:
                    next_event = 'order_cancelled'
            elif current_state == 'picked_up' and elapsed > 20:

                if random.random() < PROBABILITIES['order_delivered']:
                    next_event = 'order_delivered'
                else:
                    next_event = 'order_cancelled'
            
            if next_event:
                event = self.generate_order_event(timestamp, next_event, order_id)
                processed_event = self.error_injector.inject_errors(event)
                if processed_event:
                    events.append(processed_event)
        
        return events
    
    def _progress_existing_orders(self, timestamp: datetime) -> List[Dict[str, Any]]:
        """Progress existing orders without creating new ones"""
        events = []
        
        for order_id, order_data in list(self.active_orders.items()):
            created_at = order_data['created_at']
            current_state = order_data.get('state', 'created')
            elapsed = (timestamp - created_at).total_seconds() / 60
            
            next_event = None
            if current_state == 'created' and elapsed > 2:
                if random.random() < PROBABILITIES['order_accepted']:
                    next_event = 'order_accepted'
                    self.active_orders[order_id]['state'] = 'accepted'
                else:
                    next_event = 'order_cancelled'
            elif current_state == 'accepted' and elapsed > 8:
                if random.random() < PROBABILITIES['order_picked_up']:
                    next_event = 'order_picked_up'
                    self.active_orders[order_id]['state'] = 'picked_up'
                else:
                    next_event = 'order_cancelled'
            elif current_state == 'picked_up' and elapsed > 20:
                if random.random() < PROBABILITIES['order_delivered']:
                    next_event = 'order_delivered'
                else:
                    next_event = 'order_cancelled'
            
            if next_event:
                event = self.generate_order_event(timestamp, next_event, order_id)
                processed_event = self.error_injector.inject_errors(event)
                if processed_event:
                    events.append(processed_event)
        
        return events
    
    def get_rider_delivery_counts(self) -> Dict[str, int]:
        """Return current delivery counts for all riders"""
        return self.rider_delivery_counts.copy()


class RiderEventGenerator:
    """Generates rider lifecycle events (shift, location pings)"""
    
    def __init__(self, riders: List[Dict], zones: List[Dict]) -> None:
        self.riders = riders
        self.zones = zones
        self.error_injector = ErrorInjector()
        self.active_riders: Dict[str, Dict] = {}  # Track active rider state
    
    def generate_rider_event(
        self, 
        timestamp: datetime,
        event_type: str,
        rider: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate a single rider event"""
        
        # Get zone for GPS coordinates
        zone = next((z for z in self.zones if z['zone_id'] == rider['zone_id']), None)
        
        # Generate GPS coordinates within zone
        if zone:
            boundary = json.loads(zone['boundary_polygon'])
            lat = random.uniform(boundary[0][0], boundary[2][0])
            lon = random.uniform(boundary[0][1], boundary[2][1])
        else:
            lat = random.uniform(GPS_BOUNDS['lat_min'], GPS_BOUNDS['lat_max'])
            lon = random.uniform(GPS_BOUNDS['lon_min'], GPS_BOUNDS['lon_max'])
        
        # 50% busy, 40% idle, 10% offline (during shifts)
        if event_type == 'shift_ended':
            status = 'offline'
        elif event_type == 'shift_started':
            status = 'idle'
        elif event_type == 'order_assigned':
            status = 'busy'
        else:
            status = random.choices(
                RIDER_STATUSES,
                weights=[0.40, 0.50, 0.10]  # idle, busy, offline
            )[0]
        
        event = {
            'event_id': str(uuid.uuid4()),
            'event_timestamp': timestamp.isoformat(),
            'event_type': event_type,
            'rider_id': rider['rider_id'],
            'zone_id': rider['zone_id'],
            'latitude': round(lat, 6),
            'longitude': round(lon, 6),
            'status': status
        }
        
        return event
    
    def generate_events_for_minute(self, timestamp: datetime, active_rider_count: int = 200) -> List[Dict[str, Any]]:
        """Generate rider events for a given minute"""
        events = []
        
        # Ensure we have active riders
        if not self.active_riders:
            for rider in random.sample(self.riders, min(active_rider_count, len(self.riders))):
                self.active_riders[rider['rider_id']] = rider
                # Generate shift_started event
                event = self.generate_rider_event(timestamp, 'shift_started', rider)
                events.append(event)    
        
        # Generate location pings (every rider pings every 30 seconds = 2 per minute)
        for rider_id, rider in self.active_riders.items():
            for _ in range(2):  # 2 pings per minute
                event = self.generate_rider_event(timestamp, 'location_ping', rider)
                events.append(event)
        
        # Some riders end shifts (5% chance per minute)
        riders_to_end = [rid for rid in self.active_riders if random.random() < PROBABILITIES['rider_shift_ended']]
        for rider_id in riders_to_end[:5]:  # Max 5 per minute
            rider = self.active_riders.pop(rider_id)
            event = self.generate_rider_event(timestamp, 'shift_ended', rider)
            events.append(event)
        
        # Some riders get assigned orders 
        for rider_id in random.sample(list(self.active_riders.keys()), min(20, len(self.active_riders))):
            if random.random() < PROBABILITIES['order_assigned']:
                rider = self.active_riders[rider_id]
                event = self.generate_rider_event(timestamp, 'order_assigned', rider)
                events.append(event)
        
        return events


# =============================================================================
# Main Synthetic Data Generator
# =============================================================================

class SyntheticDataGenerator:
    """Main orchestrator for synthetic data generation"""
    
    def __init__(self, output_dir: str = 'data') -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize reference data
        self.ref_generator = ReferenceDataGenerator(str(self.output_dir / 'reference'))
        self.zones: List[Dict] = []
        self.restaurants: List[Dict] = []
        self.riders: List[Dict] = []
        
        # Event generators (initialized after reference data)
        self.order_generator: Optional[OrderEventGenerator] = None
        self.rider_generator: Optional[RiderEventGenerator] = None
    
    def initialize(self) -> Dict[str, str]:
        """Generate reference data and initialize event generators"""
        # Generate reference data
        ref_files = self.ref_generator.generate_all()
        
        # Load generated data for event generation
        self.zones = self.ref_generator.generate_zones()
        self.restaurants = self.ref_generator.generate_restaurants(self.zones)
        self.riders = self.ref_generator.generate_riders(self.zones)
        
        # Initialize event generators
        self.order_generator = OrderEventGenerator(self.restaurants, self.riders, self.zones)
        self.rider_generator = RiderEventGenerator(self.riders, self.zones)
        
        return ref_files
    
    def generate_events_for_minute(self, timestamp: datetime) -> Tuple[List[Dict], List[Dict]]:
        """Generate all events for a given minute"""
        if not self.order_generator or not self.rider_generator:
            raise RuntimeError("Call initialize() before generating events")
        
        order_events = self.order_generator.generate_events_for_minute(timestamp)
        rider_events = self.rider_generator.generate_events_for_minute(timestamp)
        
        return order_events, rider_events
    
    def generate_events(
        self, 
        start_time: datetime, 
        duration_minutes: int = 60,
        output_file: Optional[str] = None
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Generate events for a time period
        
        Args:
            start_time: Start timestamp
            duration_minutes: Duration to simulate
            output_file: Optional JSON file to write events
            
        Returns:
            Tuple of (order_events, rider_events)
        """
        if not self.order_generator or not self.rider_generator:
            raise RuntimeError("Call initialize() before generating events")
        
        all_order_events = []
        all_rider_events = []
        
        current_time = start_time
        
        for minute in range(duration_minutes):
            timestamp = current_time + timedelta(minutes=minute)
            
            # Generate events for this minute
            order_events, rider_events = self.generate_events_for_minute(timestamp)

            all_order_events.extend(order_events)
            all_rider_events.extend(rider_events)
            
            if minute % 10 == 0:
                print(f"[{timestamp}] Generated {len(order_events)} order events, {len(rider_events)} rider events")

        # Process remaining active orders to completion
        extra_minutes = 0
        while self.order_generator.active_orders and extra_minutes < 35:
            extra_minutes += 1
            timestamp = current_time + timedelta(minutes=duration_minutes + extra_minutes)
            # Only progress existing orders, don't create new ones
            order_events = self.order_generator._progress_existing_orders(timestamp)
            all_order_events.extend(order_events)
        
        # Write to file if specified
        if output_file:
            events_dir = self.output_dir / 'events'
            events_dir.mkdir(exist_ok=True)
            
            with open(events_dir / f'{output_file}_orders.json', 'w') as f:
                json.dump(all_order_events, f, indent=2)
            
            with open(events_dir / f'{output_file}_riders.json', 'w') as f:
                json.dump(all_rider_events, f, indent=2)
        
        self.export_updated_riders()    
        
        return all_order_events, all_rider_events
    
    def get_updated_riders(self) -> List[Dict[str, Any]]:
        """Get riders with updated delivery counts"""
        if not self.order_generator:
            return self.riders
        
        counts = self.order_generator.get_rider_delivery_counts()
        updated_riders = []
        for rider in self.riders:
            updated_rider = rider.copy()
            updated_rider['total_deliveries'] = counts.get(rider['rider_id'], rider['total_deliveries'])
            updated_riders.append(updated_rider)
        return updated_riders
    
    def export_updated_riders(self, filename: str = 'riders.csv') -> str:
        """Export riders with updated delivery counts to CSV"""
        updated_riders = self.get_updated_riders()
        return self.ref_generator.write_csv(updated_riders, filename)
    
    def get_stats(self, order_events: List[Dict], rider_events: List[Dict]) -> Dict[str, Any]:
        """Calculate statistics for generated events"""
        
        # Order stats
        order_types = {}
        regions = {}
        payment_methods = {}
        malformed_orders = 0
        null_orders = 0

        for event in order_events:
            if isinstance(event, str):
                malformed_orders += 1
                continue  # Skip malformed entries
            event_type = event.get('event_type', 'unknown')
            order_types[event_type] = order_types.get(event_type, 0) + 1
            
            region = event.get('region', 'unknown')
            regions[region] = regions.get(region, 0) + 1
            
            pm = event.get('payment_method', 'unknown')
            payment_methods[pm] = payment_methods.get(pm, 0) + 1

            if any(v is None for k, v in event.items() if k!="rider_id"):
                null_orders += 1
        
        # Rider stats
        rider_types = {}
        rider_statuses = {}
        
        for event in rider_events:
            if isinstance(event, str):
                malformed_orders += 1
                continue  # Skip malformed entries
            event_type = event.get('event_type', 'unknown')
            rider_types[event_type] = rider_types.get(event_type, 0) + 1
            
            status = event.get('status', 'unknown')
            rider_statuses[status] = rider_statuses.get(status, 0) + 1

            if any(v is None for v in event.values()):
                null_orders += 1
        
        
        return {
            'total_order_events': len(order_events),
            'total_rider_events': len(rider_events),
            'order_event_types': order_types,
            'regions': regions,
            'payment_methods': payment_methods,
            'rider_event_types': rider_types,
            'rider_statuses': rider_statuses,
            'errors': {
                'malformed_json': malformed_orders,
                'null_values': null_orders
            }
        }


def main() -> None:
    """Main entry point for synthetic data generation"""
    print("=" * 60)
    print("Delivery Platform Synthetic Data Generator")
    print("=" * 60)
    
    # Initialize generator
    generator = SyntheticDataGenerator(output_dir='data')
    
    print("\n[1/3] Generating reference data...")
    ref_files = generator.initialize()
    for name, path in ref_files.items():
        print(f"  - {name}: {path}")
    
    print("\n[2/3] Generating operational events...")
    start_time = datetime.now()
    order_events, rider_events = generator.generate_events(
        start_time=start_time,
        duration_minutes=360,
        output_file='simulation_1h'
    )

    
    print("\n[3/3] Computing statistics...")
    stats = generator.get_stats(order_events, rider_events)
    
    print("\n" + "=" * 60)
    print("Generation Summary")
    print("=" * 60)
    print(f"Total Order Events: {stats['total_order_events']}")
    print(f"Total Rider Events: {stats['total_rider_events']}")
    print("\nOrder Event Types:")
    for event_type, count in stats['order_event_types'].items():
        print(f"  - {event_type}: {count}")
    print("\nRider Event Types:")
    for event_type, count in stats['rider_event_types'].items():
        print(f"  - {event_type}: {count}")
    print("\nInjected Errors:")
    for error_type, count in stats['errors'].items():
        print(f"  - {error_type}: {count}")
    
    print("\n Data generation complete!")
    print(f"  Output directory: {generator.output_dir.absolute()}")


if __name__ == "__main__":
    main()
