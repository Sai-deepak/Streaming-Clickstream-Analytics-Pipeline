import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

class ClickstreamProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='clickstream'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        
        # Sample data for realistic clickstream
        self.pages = [
            '/home', '/products', '/cart', '/checkout', '/profile',
            '/search', '/category/electronics', '/category/clothing',
            '/product/123', '/product/456', '/about', '/contact'
        ]
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        
        self.referrers = [
            'google.com', 'facebook.com', 'twitter.com', 'direct', 'email'
        ]

    def generate_click_event(self):
        """Generate a realistic click event"""
        return {
            'timestamp': datetime.now().isoformat(),
            'user_id': fake.uuid4(),
            'session_id': fake.uuid4()[:8],
            'page_url': random.choice(self.pages),
            'referrer': random.choice(self.referrers),
            'user_agent': random.choice(self.user_agents),
            'ip_address': fake.ipv4(),
            'country': fake.country_code(),
            'city': fake.city(),
            'device_type': random.choice(['desktop', 'mobile', 'tablet']),
            'duration_seconds': random.randint(5, 300),
            'is_bounce': random.choice([True, False]),
            'conversion_value': random.uniform(0, 500) if random.random() < 0.1 else 0
        }

    def start_producing(self, events_per_second=10):
        """Start producing clickstream events"""
        print(f"Starting to produce {events_per_second} events per second...")
        
        try:
            while True:
                event = self.generate_click_event()
                self.producer.send(self.topic, value=event)
                print(f"Sent: {event['user_id'][:8]} -> {event['page_url']}")
                
                time.sleep(1 / events_per_second)
                
        except KeyboardInterrupt:
            print("Stopping producer...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    producer = ClickstreamProducer()
    producer.start_producing(events_per_second=5)