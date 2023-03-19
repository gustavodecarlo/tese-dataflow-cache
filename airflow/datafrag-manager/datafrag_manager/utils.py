import re
import logging

import boolean
from sqloxide import parse_sql
from pyspark.sql.session import SparkSession, DataFrame


logger = logging.getLogger(__name__)

def nested_dict_pairs_iterator(dict_obj):
    ''' 
        This function accepts a nested dictionary as argument
        and iterate over all values of nested dictionaries
    '''
    
    for key, value in dict_obj.items():
        if isinstance(value, dict):
            for pair in nested_dict_pairs_iterator(value):
                yield (key, *pair)
        else:
            yield (key, value)


def convert_filter_to_dnf(filter: str) -> str:
    algebra = boolean.BooleanAlgebra()
    m = re.finditer(r'([A-Za-z0-9\#]+)\s*(?:=|<=|>=|>|<)\s*(?:\d+(?:\.\d+)?)|(and|or|AND|OR)\s+|(\w+\([A-Za-z0-9\#]+\))', filter)
    elements = {}
    old_element = ''
    idx = 0 
    for o in m:
        if '#' in o.group():
            element = re.findall(r'[A-Za-z0-9\#]+', re.sub(r'\w+\(','',o.group()))[0].split('#')[0]
            element_idx = f'{element}{idx}'
            term = o.group()
            if old_element == element:
                term = f'{old_term} AND {o.group()}'
            elements[element_idx] = f'({term})'
            filter = filter.replace(o.group(), element_idx)
            old_element = element
            old_term = o.group()
            idx += 1

    expression = algebra.parse(filter, simplify=False)
    exprdnf = algebra.dnf(expression)
    strdnf = str(exprdnf)
    logger.debug(f'Boolean expression: {expression}')
    logger.debug(f'Boolean DNF expression: {exprdnf}')
    for key, value in elements.items():
        strdnf = strdnf.replace(key, value)
    strdnf = strdnf.replace('&', ' AND ').replace('|', ' OR ')
    return strdnf

def get_metadata_table_representation(spark_session: SparkSession, dataflow: str, task: str, datasource: str, filter: str) -> DataFrame:
    logger.info(f'filter: {filter}')
    dnf_filter = convert_filter_to_dnf(filter)
    sql = f'''
        SELECT *
        FROM {dataflow}
        WHERE {dnf_filter}
    '''
    output = parse_sql(sql=sql, dialect='spark')
    where_condition = output[0].get('Query').get('body').get('Select').get('selection')
    old_term = None
    table_repr = [{}]
    term = 1
    for pair in nested_dict_pairs_iterator(where_condition):
        if 'value' in pair[-2]:
            if old_term and old_term != pair[-1]:
                table_repr.append({})
            table_repr[len(table_repr)-1].update({'dataflow': dataflow})
            table_repr[len(table_repr)-1].update({'task': task})
            table_repr[len(table_repr)-1].update({'datasource': datasource})
            table_repr[len(table_repr)-1].update({'attribute': pair[-1].split('#')[0]})
            old_term = pair[-1]
        if 'op' in pair[-2]:
            # tratar o novo termo e adicionando a lista esse novo elemento
            if 'Or' in pair[-1]:
                table_repr.append({})
                term = term + 1
                old_term = None
            
            table_repr[len(table_repr)-1].update({'term': term})
            operation = pair[-1]
        if 'Number' in pair[-2]:
            if 'GtEq' in operation or 'Gt' in operation:
                table_repr[len(table_repr)-1].update({'min': float(pair[-1][0])})
            if operation == 'Eq':
                table_repr[len(table_repr)-1].update({'min': float(pair[-1][0])})
                table_repr[len(table_repr)-1].update({'max': float(pair[-1][0])})
            if 'LtEq' in operation or 'Lt' in operation:
                table_repr[len(table_repr)-1].update({'max': float(pair[-1][0])})
            
    logger.debug(table_repr)
    return spark_session.createDataFrame(data=table_repr)