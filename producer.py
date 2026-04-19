import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# 1. Connect to your local Kafka Server
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'], 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Successfully connected to Kafka!")
except Exception as e:
    print(f"❌ Make sure your Kafka server is running! Error: {e}")
    exit()

# 2. Define the Fake Data Generator (Upgraded for Fraud Detection)
def generate_sale():
    products = ['Laptop', 'Smartphone', 'Headphones', 'Smartwatch', 'Tablet']
    
    # Normal price generation
    price = round(random.uniform(50.0, 1200.0), 2)
    
    # 💡 UPGRADE: 5% chance to generate a massive "Suspicious" transaction over $2500
    if random.random() < 0.05:
        price = round(random.uniform(2600.0, 5000.0), 2)
        
    return {
        'order_id': random.randint(10000, 99999),
        'product': random.choice(products),
        'price': price,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

# 3. Start the "Flash Sale"
print("🚀 Flash Sale Started! Generating orders...")
try:
    while True:
        sale_data = generate_sale()
        
        # Send the data to Kafka
        producer.send('sales_topic', value=sale_data)
        
        # Print to terminal, highlighting the fraudulent ones!
        if sale_data['price'] >= 2500.0:
            print(f"🚨 SUSPICIOUS ORDER PLACED: {sale_data}")
        else:
            print(f"Order placed: {sale_data}")
        
        # Pause for half a second
        time.sleep(0.5) 
        
except KeyboardInterrupt:
    print("\n🛑 Flash Sale Ended.")
finally:
    producer.close()