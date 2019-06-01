from datetime import datetime
from main import (
    get_dataframe, data,
    get_data,
    join,
    get_nested_groups_dataframe,
    get_state,
    get_sensor_group_dataframe,
    get_all_nested_sensors_for_group
)


sensor_to_group_mapping = {
    0: 0,
    1: 1,
    2: 2,
    3: 3,
}


child_to_parent_group_mapping = {
    1: 0,
    2: 1,
    3: 2,
}


def test_main():
    data = get_data()
    df = get_dataframe(data)
    group_params = {
        'sensor_to_group_mapping': sensor_to_group_mapping,
        'child_to_parent_group_mapping': child_to_parent_group_mapping,
    }
    filters = {'filters': None}
    result = get_state(df, 'group', {**filters, **group_params})
    print(result)
    assert False
