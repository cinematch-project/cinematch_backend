from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field


class DBVersion(SQLModel, table=True):
    """
    Single-row table to store DB schema/data version
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    version: int
    updated_at: Optional[datetime] = None
