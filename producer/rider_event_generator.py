from typing import List, Dict, Tuple, Optional, Any
import json
import random
import uuid
from datetime import datetime
from error_injection import ErrorInjector
from config_constants import *

class RiderEventGenerator:
    """Generates rider lifecycle events (shift, location pings)"""
    
    def __init__(self, riders: List[Dict], zones: List[Dict], ref_generator: Optional['ReferenceDataGenerator'] = None) -> None:
        self.all_riders = riders  # Reference to full rider list (shared with OrderEventGenerator)
        self.zones = zones
        self.error_injector = ErrorInjector()
        self.active_riders: Dict[str, Dict] = {}  # Track active rider state (currently on shift)
        self.riders_left: List[Dict] = []  # Track riders who permanently left
        self.new_riders_joined: List[Dict] = []  # Track new riders who joined during simulation
        self.ref_generator = ref_generator  # For generating new riders
        self._next_rider_number = len(riders) + 1  # For generating unique rider IDs
        
        # Cumulative stats for streaming mode (not cleared on flush)
        self.total_riders_left = 0
        self.total_new_riders = 0
    
    @property
    def available_riders(self) -> List[Dict]:
        """Get riders who haven't left and are available to start shifts"""
        return [r for r in self.all_riders if not r.get('has_left', False)]
    
    def add_new_rider(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Add a new rider to the platform and generate shift_started event"""
        if not self.ref_generator:
            return None
        
        new_rider = self.ref_generator.generate_single_rider(self.zones, self._next_rider_number)
        self._next_rider_number += 1
        
        # Add to tracking lists
        self.all_riders.append(new_rider)
        self.new_riders_joined.append(new_rider)
        self.active_riders[new_rider['rider_id']] = new_rider
        self.total_new_riders += 1
        
        # Generate shift_started event for new rider
        return self.generate_rider_event(timestamp, 'shift_started', new_rider)
    
    def mark_rider_left(self, rider_id: str) -> None:
        """Mark a rider as permanently left"""
        for rider in self.all_riders:
            if rider['rider_id'] == rider_id:
                rider['has_left'] = True
                self.riders_left.append(rider)
                self.total_riders_left += 1
                break
    
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
    
    def generate_events_for_minute(self, timestamp: datetime, active_rider_count: int = 200) -> Tuple[List[Dict[str, Any]], List[str], List[Dict]]:
        """Generate rider events for a given minute
        
        Returns:
            Tuple of (events, riders_who_left_ids, new_riders_joined)
        """
        events = []
        riders_left_this_minute = []
        new_riders_this_minute = []
        
        # Ensure active riders (only from available riders who haven't left)
        if not self.active_riders:
            available = self.available_riders
            for rider in random.sample(available, min(active_rider_count, len(available))):
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
            
            # After ending shift (now offline), small chance rider leaves permanently
            if random.random() < PROBABILITIES['rider_leaves_after_shift']:
                self.mark_rider_left(rider_id)
                riders_left_this_minute.append(rider_id)
        
        # New riders can join (small probability per minute)
        if random.random() < PROBABILITIES['new_rider_joins']:
            new_rider_event = self.add_new_rider(timestamp)
            if new_rider_event:
                events.append(new_rider_event)
                new_riders_this_minute.append(self.new_riders_joined[-1])
        
        # Some riders get assigned orders (only active riders who haven't left)
        active_rider_ids = list(self.active_riders.keys())
        if active_rider_ids:
            for rider_id in random.sample(active_rider_ids, min(20, len(active_rider_ids))):
                if random.random() < PROBABILITIES['order_assigned']:
                    rider = self.active_riders[rider_id]
                    event = self.generate_rider_event(timestamp, 'order_assigned', rider)
                    events.append(event)
        
        return events, riders_left_this_minute, new_riders_this_minute
    
    def clear_pending_changes(self) -> Tuple[int, int]:
        """Clear tracking lists after CSV flush. Returns counts before clearing."""
        left_count = len(self.riders_left)
        joined_count = len(self.new_riders_joined)
        self.riders_left.clear()
        self.new_riders_joined.clear()
        return left_count, joined_count
    
    def has_pending_changes(self) -> bool:
        """Check if there are rider changes pending CSV flush"""
        return len(self.riders_left) > 0 or len(self.new_riders_joined) > 0



if __name__=='__main__':
    from reference_data_generator import ReferenceDataGenerator
    
    