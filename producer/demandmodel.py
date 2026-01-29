import numpy as np
from scipy.stats import norm, gamma
from datetime import datetime, timedelta
from typing import Dict

np.random.seed(42)

class DemandModel:
    """
    Multi-layer demand model for delivery platform
    Uses Poisson base, bimodal Gaussian hourly patterns, 
    categorical day-of-week effects, and gamma-distributed shocks
    """
    
    def __init__(self, base_lambda: float = 50) -> None:
        """
        Args:
            base_lambda: Base order arrival rate (orders/minute)
        """
        self.base_lambda = base_lambda//2
        
        # Day-of-week multipliers (0=Monday, 6=Sunday)
        self.day_multipliers: Dict[int, float] = {
            0: 0.85,  # Monday
            1: 0.95,  # Tuesday
            2: 1.00,  # Wednesday (baseline)
            3: 1.05,  # Thursday
            4: 1.20,  # Friday
            5: 1.40,  # Saturday (peak)
            6: 1.30   # Sunday
        }
    
    def hourly_multiplier(self, hour: int) -> float:
        """
        Bimodal Gaussian mixture for lunch (13:00) and dinner (20:00) peaks
        
        Args:
            hour: Hour of day (0-23)
            
        Returns:
            Demand multiplier (0.1 to 3.0)
        """
        # Lunch peak: mean=13:00, std=1 hour
        lunch_peak: np.ndarray = norm.pdf(hour, loc=13, scale=1.0)
        
        # Dinner peak: mean=20:00, std=1.2 hours (wider)
        dinner_peak: np.ndarray = norm.pdf(hour, loc=20, scale=1.2)
        
        # Dinner typically approx 1.5x lunch volume
        combined = lunch_peak + (np.random.uniform(1.0, 1.6) * dinner_peak)
        
        # Normalize to range [0.1, 3.0]
        normalized: float = 0.1 + (combined / combined.max()) * 2.9
        
        return normalized
    
    def get_day_multiplier(self, date: datetime) -> float:
        """
        Categorical multiplier based on day of week
        
        Args:
            date: datetime object
            
        Returns:
            Day-of-week multiplier
        """
        return self.day_multipliers[date.weekday()]
    
    def demand_shock(self) -> float:
        """
        Heavy-tailed shock for festivals/weather events
        Using Gamma distribution for asymmetric spikes
        
        Returns:
            Shock multiplier (usually ~1.0, occasionally 2-5x)
        """
        # Gamma distribution: mean=1.0, allows for 3-5x spikes
        return gamma.rvs(a=2, scale=0.5)
    
    def generate_order_rate(self, timestamp: datetime) -> int:
        """
        Generate order count for a given minute combining all factors
        
        Args:
            timestamp: Current timestamp
            
        Returns:
            Number of orders for this minute
        """
        hour = timestamp.hour
        
        # Component multipliers
        day_mult = self.get_day_multiplier(timestamp)
        time_mult = self.hourly_multiplier(hour)
        
        # 1% probability of shock event
        shock_mult = self.demand_shock() if np.random.random() < 0.01 else 1.0
        
        # Effective rate combining all factors
        effective_rate = self.base_lambda * day_mult * time_mult * shock_mult
        
        # Generate Poisson-distributed order count
        return np.random.poisson(effective_rate)
    
    def generate_inter_arrival_times(self, count: int) -> np.ndarray:
        """
        Generate exponential inter-arrival times (seconds)
        
        Args:
            count: Number of inter-arrival times to generate
            
        Returns:
            Array of inter-arrival times in seconds
        """
        # Convert orders/minute to orders/second
        rate_per_second = self.base_lambda / 60
        return np.random.exponential(1 / rate_per_second, size=count)

def main() -> None:
    model = DemandModel(base_lambda=50)
    current_time = datetime.now().replace(second=0, microsecond=0)
    
    # Simulate order counts for the next 120 minutes
    for minute_offset in range(120):
        timestamp = current_time + timedelta(minutes=minute_offset)
        orders = model.generate_order_rate(timestamp)
        print(f"{timestamp}: {orders} orders")

if __name__ == "__main__":
    main()