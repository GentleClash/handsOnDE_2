"""
Synthetic Data Generator for Delivery Platform Events

Generates realistic operational events (orders, riders) and reference data (CSV)
"""

import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

from reference_data_generator import ReferenceDataGenerator
from order_event_generator import OrderEventGenerator
from rider_event_generator import RiderEventGenerator

from config_constants import *

random.seed(42)

class SyntheticDataGenerator:
    """Main orchestrator for synthetic data generation"""
    
    def __init__(self, 
                 output_dir: str = 'data', 
                 csv_flush_interval: int = 60
        ) -> None:
        """
        Args:
            output_dir: Directory for output files
            csv_flush_interval: Minutes between CSV flushes in streaming mode (0 to disable)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Reference data
        self.ref_generator = ReferenceDataGenerator(str(self.output_dir / 'reference'))
        self.zones: List[Dict] = []
        self.restaurants: List[Dict] = []
        self.riders: List[Dict] = []
        
        # Event generators
        self.order_generator: Optional[OrderEventGenerator] = None
        self.rider_generator: Optional[RiderEventGenerator] = None
        
        # Streaming mode config
        self.csv_flush_interval = csv_flush_interval
        self._minutes_since_flush = 0
        self._last_flush_time: Optional[datetime] = None
        
        # Incremental stats for streaming mode
        self._stats = {
            'total_order_events': 0,
            'total_rider_events': 0,
            'order_event_types': {},
            'rider_event_types': {},
            'regions': {},
            'payment_methods': {},
            'rider_statuses': {},
            'errors': {'malformed_json': 0, 'null_values': 0}
        }
    
    def initialize(self) -> Dict[str, str]:
        """Generate reference data and initialize event generators"""
        # Generate reference data
        ref_files = self.ref_generator.generate_all()
        
        # Load generated data for event generation
        self.zones = self.ref_generator.generate_zones()
        self.restaurants = self.ref_generator.generate_restaurants(self.zones)
        self.riders = self.ref_generator.generate_riders(self.zones)
        
        # Initialize event generators - they share the same riders list
        # so changes in one are reflected in the other
        self.order_generator = OrderEventGenerator(
            self.restaurants, 
            self.riders, 
            self.zones)
        
        self.rider_generator = RiderEventGenerator(
            self.riders, 
            self.zones, 
            ref_generator=self.ref_generator
        )
        
        return ref_files
    
    def generate_events_for_minute(self, 
                                   timestamp: datetime, 
                                   auto_flush: bool = True
                                ) -> Tuple[List[Dict], List[Dict]]:
        """Generate all events for a given minute
        
        Args:
            timestamp: Current simulation time
            auto_flush: If True, automatically flush CSV at configured interval
            
        Returns:
            Tuple of (order_events, rider_events)
        """
        if not self.order_generator or not self.rider_generator:
            raise RuntimeError("Call initialize() before generating events")
        
        order_events = self.order_generator.generate_events_for_minute(timestamp)
        rider_events, riders_left, new_riders = self.rider_generator.generate_events_for_minute(timestamp)
        
        # Sync rider state: notify order generator about riders leaving
        for rider_id in riders_left:
            self.order_generator.mark_rider_left(rider_id)
        
        # Sync rider state: notify order generator about new riders
        for new_rider in new_riders:
            self.order_generator.add_rider(new_rider)
        
        # Update incremental stats
        self._update_incremental_stats(order_events, rider_events)
        
        # Periodic CSV flush for streaming mode
        self._minutes_since_flush += 1
        if auto_flush and self.csv_flush_interval > 0:
            if self._minutes_since_flush >= self.csv_flush_interval:
                self.flush_rider_csv(timestamp)
        
        return order_events, rider_events
    
    def _update_incremental_stats(self, 
                                  order_events: List[Dict], 
                                  rider_events: List[Dict]
                                ) -> None:
        """Update running stats incrementally (for streaming mode)"""
        self._stats['total_order_events'] += len(order_events)
        self._stats['total_rider_events'] += len(rider_events)
        
        for event in order_events:
            if isinstance(event, str):
                self._stats['errors']['malformed_json'] += 1
                continue
            
            et = event.get('event_type', 'unknown')
            self._stats['order_event_types'][et] = self._stats['order_event_types'].get(et, 0) + 1
            
            region = event.get('region', 'unknown')
            self._stats['regions'][region] = self._stats['regions'].get(region, 0) + 1
            
            pm = event.get('payment_method', 'unknown')
            self._stats['payment_methods'][pm] = self._stats['payment_methods'].get(pm, 0) + 1
            
            if any(v is None for k, v in event.items() if k != 'rider_id'):
                self._stats['errors']['null_values'] += 1
        
        for event in rider_events:
            if isinstance(event, str):
                self._stats['errors']['malformed_json'] += 1
                continue
            
            et = event.get('event_type', 'unknown')
            self._stats['rider_event_types'][et] = self._stats['rider_event_types'].get(et, 0) + 1
            
            status = event.get('status', 'unknown')
            self._stats['rider_statuses'][status] = self._stats['rider_statuses'].get(status, 0) + 1
            
            if any(v is None for v in event.values()):
                self._stats['errors']['null_values'] += 1
    
    def flush_rider_csv(
            self, 
            timestamp: Optional[datetime] = None
        ) -> str:
        """Flush rider changes to CSV and clear pending tracking lists
                
        Returns:
            Path to the written CSV file
        """
        if not self.rider_generator:
            raise RuntimeError("Call initialize() before flushing")
        
        # Export current state
        filepath = self.export_updated_riders()
        
        # Clear tracking lists to free memory
        left_count, joined_count = self.rider_generator.clear_pending_changes()
        
        # Reset flush counter
        self._minutes_since_flush = 0
        self._last_flush_time = timestamp
        
        if left_count > 0 or joined_count > 0:
            ts_str = timestamp.isoformat() if timestamp else 'now'
            print(f"[{ts_str}] CSV flushed: {joined_count} new riders, {left_count} riders left")
        
        return filepath
    
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
                riders_left_count = len(self.rider_generator.riders_left) if self.rider_generator else 0
                new_riders_count = len(self.rider_generator.new_riders_joined) if self.rider_generator else 0
                print(f"[{timestamp}] Generated {len(order_events)} order events, {len(rider_events)} rider events | Riders left: {riders_left_count}, New riders: {new_riders_count}")

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
        """Get riders with updated delivery counts and has_left status"""
        if not self.order_generator:
            return self.riders
        
        counts = self.order_generator.get_rider_delivery_counts()
        updated_riders = []
        
        # Shared riders list which includes new riders and has_left updates
        all_riders = self.order_generator.all_riders
        
        for rider in all_riders:
            updated_rider = rider.copy()
            updated_rider['total_deliveries'] = counts.get(rider['rider_id'], rider.get('total_deliveries', 0))
            updated_riders.append(updated_rider)
        
        return updated_riders
    
    def export_updated_riders(self, filename: str = 'riders.csv') -> str:
        """Export riders with updated delivery counts to CSV"""
        updated_riders = self.get_updated_riders()
        return self.ref_generator.write_csv(updated_riders, filename, update_riders=True)
    
    def get_streaming_stats(self) -> Dict[str, Any]:
        """Get current stats for streaming mode (incremental, no event lists needed)"""
        stats = self._stats.copy()
        
        # Add rider churn from cumulative counters
        if self.rider_generator:
            stats['rider_churn'] = {
                'riders_left': self.rider_generator.total_riders_left,
                'new_riders_joined': self.rider_generator.total_new_riders,
                'net_change': self.rider_generator.total_new_riders - self.rider_generator.total_riders_left,
                'pending_flush': self.rider_generator.has_pending_changes()
            }
            stats['active_riders_count'] = len(self.rider_generator.active_riders)
            stats['total_riders_in_system'] = len(self.rider_generator.all_riders)
        
        if self.order_generator:
            stats['active_orders_count'] = len(self.order_generator.active_orders)
        
        stats['last_flush_time'] = self._last_flush_time.isoformat() if self._last_flush_time else None
        stats['minutes_since_flush'] = self._minutes_since_flush
        
        return stats
    
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
        
        # Rider churn stats
        riders_left_count = len(self.rider_generator.riders_left) if self.rider_generator else 0
        new_riders_count = len(self.rider_generator.new_riders_joined) if self.rider_generator else 0
        
        return {
            'total_order_events': len(order_events),
            'total_rider_events': len(rider_events),
            'order_event_types': order_types,
            'regions': regions,
            'payment_methods': payment_methods,
            'rider_event_types': rider_types,
            'rider_statuses': rider_statuses,
            'rider_churn': {
                'riders_left': riders_left_count,
                'new_riders_joined': new_riders_count,
                'net_change': new_riders_count - riders_left_count
            },
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
