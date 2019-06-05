from datetime import datetime
from main import (
    get_dummy_test_results,
    get_test_result_dataframe,
    get_nested_groups_dataframe,
    get_sensor_group_dataframe,
    get_all_nested_sensors_for_group,
    get_state,
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
    test_results = get_dummy_test_results()
    df = get_test_result_dataframe(test_results)
    group_params = {
        # 'sensor_to_group_mapping': sensor_to_group_mapping,
        # 'child_to_parent_group_mapping': child_to_parent_group_mapping,
    }
    filters = {'filters': None}
    result = get_state(df, ['test', 'network'], {**filters, **group_params})
    print(result)
    assert False
