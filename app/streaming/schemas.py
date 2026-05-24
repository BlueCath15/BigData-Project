from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator
)

from typing import Optional
from datetime import datetime
import re


ALLOWED_CURRENCIES = {
    "USD",
    "EUR",
    "COP",
    "GBP",
    "MXN",
    "BRL"
}

ALLOWED_TRANSACTION_TYPES = {
    "TRANSFER",
    "PAYMENT",
    "WITHDRAWAL",
    "DEPOSIT"
}


class StreamTransaction(BaseModel):

    user_id: str = Field(
        ...,
        min_length=1,
        max_length=64
    )

    amount: float = Field(
        ...,
        gt=0
    )

    transaction_type: str

    currency: str = "USD"

    destination_id: Optional[str] = Field(
        default=None,
        max_length=64
    )

    ip_address: Optional[str] = None

    created_at: datetime


    # ─────────────────────────────────────────────
    # Currency validation
    # ─────────────────────────────────────────────

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str):

        v = v.upper().strip()

        if v not in ALLOWED_CURRENCIES:
            raise ValueError(
                f"Unsupported currency: {v}"
            )

        return v


    # ─────────────────────────────────────────────
    # Transaction type validation
    # ─────────────────────────────────────────────

    @field_validator("transaction_type")
    @classmethod
    def validate_transaction_type(cls, v: str):

        v = v.upper().strip()

        if v not in ALLOWED_TRANSACTION_TYPES:
            raise ValueError(
                f"Unsupported transaction type: {v}"
            )

        return v


    # ─────────────────────────────────────────────
    # User ID validation
    # ─────────────────────────────────────────────

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str):

        v = v.strip()

        if not v:
            raise ValueError(
                "user_id cannot be empty"
            )

        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "user_id contains invalid characters"
            )

        return v


    # ─────────────────────────────────────────────
    # Destination ID validation
    # ─────────────────────────────────────────────

    @field_validator("destination_id")
    @classmethod
    def validate_destination_id(
        cls,
        v: Optional[str]
    ):

        if v is None:
            return v

        v = v.strip()

        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "destination_id contains invalid characters"
            )

        return v


    # ─────────────────────────────────────────────
    # Business rules validation
    # ─────────────────────────────────────────────

    @model_validator(mode="after")
    def validate_business_rules(self):

        # TRANSFER requires destination
        if (
            self.transaction_type == "TRANSFER"
            and not self.destination_id
        ):
            raise ValueError(
                "TRANSFER requires destination_id"
            )

        # destination cannot equal source
        if (
            self.destination_id
            and self.destination_id == self.user_id
        ):
            raise ValueError(
                "destination_id cannot equal user_id"
            )

        return self