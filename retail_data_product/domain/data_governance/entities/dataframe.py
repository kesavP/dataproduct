from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from ....application.ports import DataFrameEngine


@dataclass
class Dataframe:
    """
    Entity representing a dataframe with a unique identity.

    This wraps a backend dataframe engine (e.g., Pandas, Spark) and provides
    a domain-level abstraction with a unique identifier.

    Attributes:
        backend_dataframe (DataFrameEngine): Actual Dataframe object that will be used to
        do the data operations
    """

    backend_dataframe: "DataFrameEngine"
    id: str = field(init=False)

    def __post_init__(self):
        """
        Generate a unique ID for this dataframe entity.
        """
        self.id = str(uuid.uuid1())

    def get_backend_dataframe(self) -> "DataFrameEngine":
        """
        Get the underlying backend dataframe engine.
        """
        return self.backend_dataframe

    def __getattr__(self, name):
        """
        Delegate attribute access to the backend dataframe engine.
        """
        return getattr(self.backend_dataframe, name)
