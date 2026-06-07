from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db_session
from app.schemas import TransactionReponse, TranscationCreate
from app.models import TransactionModel


app = FastAPI(
    title="Payment Gateway - Transactions API",
    description="API for receiving and orchestrating financial transactions",
    version="1.0.0"
)

@app.post(
    "/v1/transactions",
    response_model=TransactionReponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new payment transaction"
)
async def create_transaction(
    payload: TranscationCreate,
    db: AsyncSession = Depends(get_db_session)
):
    try:
        new_transaction = TransactionModel(
            account_id=payload.account_id,
            amount=payload.amount,
            payment_method=payload.payment_method.lower()
        )

        db.add(new_transaction)
        await db.commit()
        await db.refresh(new_transaction)

        return new_transaction
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail= f"Failed to process transaction: {str(e)}"
        )
    

@app.get("health", status_code=status.HTTP_200_OK, summary="API health check")
async def health_check():
    return {status: "Healthy"}