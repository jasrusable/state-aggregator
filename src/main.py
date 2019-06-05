import pandas as pd
from uuid import uuid4
from datetime import datetime
from itertools import groupby


def with_timing(f):
    def inner(*args, **kwargs):
        d1 = datetime.now()
        result = f(*args, **kwargs)
        d2 = datetime.now()
        print(d2 - d1)
        return result
    return inner


def get_dummy_test_results(number_of_sensors=10):
    lots_of_data = []
    for sensor in range(number_of_sensors):
        for network in range(20):
            for test in range(20):
                lots_of_data.append({'date': datetime.now(), 'sensor': sensor, 'network': network,
                                     'test': test, 'result': 'error' if test % 2 == 0 else 'good'},
                                    )
    return lots_of_data


def get_all_nested_child_groups(group, child_to_parent_group_mapping):
    mappings = [{'child': child, 'parent': parent}
                for child, parent in child_to_parent_group_mapping.items()]
    children = []

    def walk(g):
        filtered_mappings = list(filter(lambda x: x['parent'] == g, mappings))
        child_groups = [mapping['child'] for mapping in filtered_mappings]
        if child_groups:
            children.extend(child_groups)
            for child in child_groups:
                walk(child)

    walk(group)
    return children


def get_all_nested_sensors_for_group(group, sensor_to_group_mapping, child_to_parent_group_mapping):
    all_groups = get_all_nested_child_groups(
        group, child_to_parent_group_mapping)
    all_groups.extend([group])
    mappings = [{'sensor': sensor, 'group': group}
                for sensor, group in sensor_to_group_mapping.items()]
    filtered_mappings = list(
        filter(lambda x: x['group'] in all_groups, mappings))
    sensors = [mapping['sensor'] for mapping in filtered_mappings]
    return sensors


def get_test_result_dataframe(test_results):
    return pd.DataFrame(
        [[d['date'], d['sensor'], d['network'], d['test'], d['result']]
            for d in test_results],
        columns=['date', 'sensor', 'network', 'test', 'result']
    ).set_index(['sensor', 'network', 'test'], verify_integrity=True)


def get_nested_groups_dataframe(child_to_parent_group_mapping):
    all_groups = set()
    for k, v in child_to_parent_group_mapping.items():
        all_groups.add(k)
        all_groups.add(v)
    data = set()
    for group in all_groups:
        child_groups = get_all_nested_child_groups(
            group, child_to_parent_group_mapping)
        data.add((group, group))
        for child_group in child_groups:
            data.add((child_group, group))
    df = pd.DataFrame(
        [[r[0], r[1]] for r in data],
        columns=['child', 'parent']
    ).set_index(['child'])
    return df


def get_sensor_group_dataframe(sensor_to_group_mapping):
    df = pd.DataFrame(
        [[sensor, group] for sensor, group in sensor_to_group_mapping.items()],
        columns=['sensor', 'group']
    ).set_index(['sensor'])
    return df


def group_by(df, by):
    grouped_df = df.groupby(by=[*by, 'result'])['result'].count().unstack()
    return grouped_df


@with_timing
def get_state(df, by, params=None):
    params = params or {}

    filters = params.get('filters')
    if filters:
        df = df.query(filters)

    sensor_to_group_mapping = params.get('sensor_to_group_mapping')
    if sensor_to_group_mapping:
        df = df.join(get_sensor_group_dataframe(sensor_to_group_mapping))

    df = group_by(df, by)

    child_to_parent_group_mapping = params.get('child_to_parent_group_mapping')
    if child_to_parent_group_mapping:
        group_df = get_nested_groups_dataframe(child_to_parent_group_mapping)
        df = df.join(group_df, on='group')
        by.remove('group')
        by.append('parent')
        df = df.groupby(by=by).agg({'error': 'sum', 'good': 'sum'})
        df.rename_axis(index={'parent': 'group'}, inplace=True)

    return df
