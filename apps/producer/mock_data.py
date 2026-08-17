"""Realistic Mock Data Generator for E-Commerce Orders and Events in Kafka POC.
"""

import datetime
import random
import uuid
from typing import Any, Dict

SAMPLE_PRODUCTS = [
    {"product_id": "PROD-001", "name": "Cloud Compute Instance", "price": 45.50},
    {"product_id": "PROD-002", "name": "Managed Database Node", "price": 120.00},
    {"product_id": "PROD-003", "name": "Object Storage Bucket (1TB)", "price": 20.00},
    {"product_id": "PROD-004", "name": "Load Balancer Service", "price": 18.25},
    {"product_id": "PROD-005", "name": "Network Egress Package (100GB)", "price": 12.00},
    {"product_id": "PROD-006", "name": "Managed Kafka Partition Group", "price": 85.00},
]

ORDER_STATUSES = ["PENDING", "CONFIRMED", "PROCESSING", "COMPLETED"]
PAYMENT_METHODS = ["CREDIT_CARD", "PIX", "INVOICE", "GCP_BILLING"]


class MockDataGenerator:
    """Generates structured e-commerce order events with sequential IDs and metadata."""

    def __init__(self):
        self._sequence_id = 0

    def generate_order(self, custom_order_id: str = None) -> Dict[str, Any]:
        """Generates a single realistic order event payload."""
        self._sequence_id += 1
        order_id = custom_order_id or f"ORD-{uuid.uuid4().hex[:8].upper()}"
        customer_id = f"CUST-{random.randint(1000, 9999)}"

        num_items = random.randint(1, 3)
        selected_products = random.sample(SAMPLE_PRODUCTS, num_items)
        items = []
        total_amount = 0.0

        for prod in selected_products:
            qty = random.randint(1, 4)
            subtotal = round(prod["price"] * qty, 2)
            total_amount += subtotal
            items.append({
                "product_id": prod["product_id"],
                "name": prod["name"],
                "quantity": qty,
                "unit_price": prod["price"],
                "subtotal": subtotal,
            })

        order_event = {
            "sequence_id": self._sequence_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "items": items,
            "total_amount": round(total_amount, 2),
            "currency": "USD",
            "payment_method": random.choice(PAYMENT_METHODS),
            "status": random.choice(ORDER_STATUSES),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source_app": "kafka-poc-producer",
        }

        return order_event
