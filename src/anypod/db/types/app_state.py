# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false
# TODO: drop once SQLModel ships Field(Column[Any]) fix (fastapi/sqlmodel#797)

"""Application-wide state table.

This module defines the AppState table used to persist global state such as the
timestamp of the last yt-dlp update attempt. This enables rate limiting of
``--update-to`` invocations across the entire application.
"""

from datetime import datetime

from sqlalchemy import Column
from sqlmodel import Field, SQLModel

from .timezone_aware_datetime import TimezoneAwareDatetime


class AppState(SQLModel, table=True):
    """ORM model representing application-wide state.

    Attributes:
        id: Primary key for the single state row. Always "global".
        last_yt_dlp_update: Timestamp (UTC) of the last yt-dlp ``--update-to``
            attempt. Used to enforce minimum update intervals.
    """

    id: str = Field(primary_key=True, default="global")
    last_yt_dlp_update: datetime = Field(
        sa_column=Column(TimezoneAwareDatetime, nullable=False)
    )
