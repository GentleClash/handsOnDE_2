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
    'rider_shift_ended': 0.05, # riders end shifts in a minute chance
    'new_rider_joins': 0.005,  # chance per minute of a new rider joining
    'rider_leaves_after_shift': 0.005  # chance rider permanently leaves after ending shift
}
