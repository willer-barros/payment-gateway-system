import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Float, DateTime
from app.database import Base

class TransactionModel(Base):
    __tablename__ = "transactions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    account_id = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    payment_method = Column(String, nullable=False) 
    status = Column(String, default="PENDING")      
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))