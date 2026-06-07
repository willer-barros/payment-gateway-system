from pydantic import BaseModel, Field
from datetime import datetime

class TranscationCreate(BaseModel):
    account_id: str = Field(..., description="Unique identifier for the customer account", examples=["acc-123456"])
    amount: float = Field(..., gt=0, description='The transaction amount must be greater', examples=[150.75])
    payment_method: str = Field(..., description="Method used for payment: pix or credit_card", examples=["pix"])


class TransactionReponse(BaseModel):
    id: str
    account_id: str
    amount: float
    payment_method: str
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True