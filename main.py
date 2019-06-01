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


data = [
    {'date': datetime.now(), 'sensor': 'a', 'network': 'a',
     'test': 'a', 'result': True},
    {'date': datetime.now(), 'sensor': 'b', 'network': 'a',
     'test': 'a', 'result': True},
    {'date': datetime.now(), 'sensor': 'c', 'network': 'a',
     'test': 'a', 'result': False},
]


def get_data():
    lots_of_data = []
    for sensor in range(7):
        for network in range(2):
            for test in range(2):
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


def join(df, other_df):
    return df.join(other_df)


def get_dataframe(data):
    return pd.DataFrame(
        [[d['date'], d['sensor'], d['network'], d['test'], d['result']]
            for d in data],
        columns=['date', 'sensor', 'network', 'test', 'result']
    ).set_index(['sensor', 'network', 'test'], verify_integrity=True)


def get_nested_groups_dataframe(child_to_parent_group_mapping):
    all_groups = set()
    for k, v in child_to_parent_group_mapping.items():
        all_groups.add(k)
        all_groups.add(v)
    data = set()
    for group in all_groups:
        child_groups = get_all_nested_child_groups(group, child_to_parent_group_mapping)
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


def get_nested_sensor_group_dataframe(sensor_to_group_mapping, child_to_parent_group_mapping):
    all_groups = set(sensor_to_group_mapping.values())
    data = set()
    for group in all_groups:
        all_sensors_for_group = get_all_nested_sensors_for_group(
            group, sensor_to_group_mapping, child_to_parent_group_mapping)
        for sensor in all_sensors_for_group:
            data.add((sensor, group))
    df = pd.DataFrame(
        [[r[0], r[1], True] for r in data],
        columns=['sensor', 'group', 'True']
    ).set_index(['sensor', 'group'])
    return df


def group_by(df, by):
    grouped_df = df.groupby(by=[*by, 'result'])['result'].count().unstack()
    return grouped_df


def get_state(df, by, params=None):
    params = params or {}
    sensor_to_group_mapping = params.get('sensor_to_group_mapping')
    filters = params.get('filters')
    if filters:
        df = df.query(filters)
    if sensor_to_group_mapping:
        df = df.join(get_sensor_group_dataframe(sensor_to_group_mapping))
    entity_map = {
        'sensor': ['sensor'],
        'sensor_by_group': ['sensor', 'group'],
        'network': ['network'],
        'network_by_group': ['network', 'group'],
        'test': ['test'],
        'test_by_group': ['test', 'group'],
        'group': ['group'],
    }
    by = entity_map[by]
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
