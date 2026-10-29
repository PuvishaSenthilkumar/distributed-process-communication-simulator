import math

import pytest

from models import SimulationConfig


@pytest.mark.parametrize(
    "config",
    [
        SimulationConfig(process_count=2),
        SimulationConfig(process_count=101),
        SimulationConfig(process_count=3.5),
        SimulationConfig(process_count=True),
        SimulationConfig(messages_per_process=-1),
        SimulationConfig(messages_per_process=1.5),
        SimulationConfig(min_delay=-0.1),
        SimulationConfig(min_delay=0.2, max_delay=0.1),
        SimulationConfig(min_delay=math.nan),
        SimulationConfig(max_delay=math.inf),
        SimulationConfig(timeout=0),
        SimulationConfig(timeout=math.inf),
    ],
)
def test_invalid_configurations_are_rejected(config: SimulationConfig) -> None:
    with pytest.raises(ValueError):
        config.validate()
