from typing import List, Dict, Any, Optional
from datetime import datetime
import random
import uuid
from demand_model import DemandModel
from error_injection import ErrorInjector
from config_constants import *


class OrderEventGenerator:
    """Generates order lifecycle events"""
    
    def __init__(self, 
                 restaurants: List[Dict], 
                 riders: List[Dict], 
                 zones: List[Dict]
            ) -> None:
        self.restaurants = restaurants
        self.all_riders = riders  
        self.zones = zones
        self.demand_model = DemandModel(base_lambda=50)
        self.error_injector = ErrorInjector()
        self.active_orders: Dict[str, Dict] = {}  # Track order state

        self.rider_delivery_counts: Dict[str, int] = {
            rider['rider_id']: rider['total_deliveries'] for rider in riders
        }
    
    @property
    def riders(self) -> List[Dict]:
        """Get only active riders (has_left=False)"""
        return [rider for rider in self.all_riders if not rider.get('has_left', False)]
    
    def add_rider(self, rider: Dict) -> None:
        """Add a new rider to the pool"""
        self.all_riders.append(rider)
        self.rider_delivery_counts[rider['rider_id']] = rider.get('total_deliveries', 0)
    
    def mark_rider_left(self, rider_id: str) -> None:
        """Mark a rider as having left"""
        for rider in self.all_riders:
            if rider['rider_id'] == rider_id:
                rider['has_left'] = True
                break
    
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



if __name__ == '__main__':

    pass
    
