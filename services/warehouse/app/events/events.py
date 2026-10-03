from pydantic import BaseModel, ConfigDict, Field


class OrderItemEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    product_id: str = Field(min_length=1)
    sku: str = ""
    product_name: str = ""
    quantity: int = Field(gt=0)
    unit_price: float = Field(default=0, ge=0)


class OrderCreatedEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str = ""
    event_type: str = "order.created"
    schema_version: int = 1
    order_id: str = Field(min_length=1)
    user_id: str = ""
    items: list[OrderItemEvent] = Field(min_length=1)
    total_amount: float = Field(default=0, ge=0)
    currency: str = "USD"
    status: str = "pending"
    created_at: str = ""
