from typing import Dict, Optional
from .base import PartitionStrategy
from .range_strategy import RangePartitionStrategy
from .list_strategy import ListPartitionStrategy
from .hash_strategy import HashPartitionStrategy
from .default_strategy import DefaultPartitionStrategy

class PartitionStrategyRegistry:
    _strategies: Dict[str, PartitionStrategy] = {}

    @classmethod
    def register(cls, key: str, strategy: PartitionStrategy) -> None:
        cls._strategies[key] = strategy

    @classmethod
    def unregister(cls, key: str) -> None:
        cls._strategies.pop(key, None)

    @classmethod
    def get(cls, key: str) -> Optional[PartitionStrategy]:
        return cls._strategies.get(key)

    @classmethod
    def all(cls) -> Dict[str, PartitionStrategy]:
        return dict(cls._strategies)


# Register defaults
PartitionStrategyRegistry.register("r", RangePartitionStrategy())
PartitionStrategyRegistry.register("l", ListPartitionStrategy())
PartitionStrategyRegistry.register("h", HashPartitionStrategy())
PartitionStrategyRegistry.register("d", DefaultPartitionStrategy())
