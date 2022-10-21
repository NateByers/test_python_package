from __future__ import annotations
import yaml
from os import linesep, path
from pandas import DataFrame
from pyarrow import Table, concat_tables
from pyarrow.parquet import write_table, read_table
from pyarrow.csv import write_csv
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, TableName, escape_string_literal
from hashlib import md5
from datetime import date, timedelta
from sqllineage.runner import LineageRunner
from sqllineage.core.models import Schema
from enum import Enum
import os
import sqlalchemy as sa
import connectorx as cx
import duckdb
import sqlparse
from os import path, linesep

# enumerated value list of allowable timeframes
class Timeframe(Enum):
    """ Constants which define the level of granularity for a MetricInstance to be aggregated to. """

    DAILY = 1
    MONTHLY = 2
    WEEKLY = 3


def prep_sql(base_logic: str) -> str:
    """
    This takes the base SQL for a metric, parses in into tokenized pieces, and rewrites as a CTE aliased "bse".

    :param str base_logic: a text string of the base SQL for a metric
    :return: the aggregate SQL query specific to the MetricInstance
    :rtype: str
    """

    sql_file = path.join('..', 'metrics', 'sql', base_logic)
    code = open(sql_file, 'r').read()

    statements = sqlparse.split(code)
    num_statements = len(statements)
    last_statement_index = num_statements - 1
    statement = statements[last_statement_index]

    parsed = sqlparse.parse(statement)[0]
    try:
        cte_flag = max([1 for token in parsed.tokens if token.ttype == sqlparse.tokens.Token.Keyword.CTE])
    except:
        cte_flag = 0

    # find non-CTE Select statement location
    for idx, token in enumerate(parsed.tokens):
        if token.ttype == sqlparse.tokens.Token.Keyword.DML:
            if cte_flag == 1:
                new_token_text = f',bse as {linesep}({linesep}'
            else:
                new_token_text = f'with bse as {linesep}({linesep}'

            new_token = sqlparse.sql.Token(sqlparse.tokens.Token.String, new_token_text)
            token_loc = idx

    parsed.insert_before(token_loc, new_token)
    return str(parsed) + f'{linesep})'



def make_mssql_connection(
        connection_type: str,
        database_name: str,
        hostname: str = 'P-WEDWSQLCS',
        odbc_driver: str = 'ODBC Driver 17 for SQL Server'
) -> str:
    """
    Optional helper function to create a sqlalchemy connection with Jeremy's recommended properties.

    :param str connection_type: 'connectorx' or 'sqlalchemy' depending on type of connection being established
    :param str database_name: user input string for name of database in EDW to execute SQL from
    :param str hostname: hostname for the EDW server to execute from, defaults to production EDW hostname
    :param str odbc_driver: ODBC driver version to use for SQL Server connections
    :return: a connection string for either a connectorx connection or sqlalchemy connection to the EDW
    :rtype: str
    """

    if connection_type.lower() in ['sqlalchemy', 'sa', 'sql_alchemy']:
        connection_string = f'mssql+pyodbc://{hostname}/{database_name}?driver={odbc_driver}'
    elif connection_type.lower() in ['connectorx', 'cx', 'connector_x']:
        connection_string = f'mssql://{hostname}/{database_name}?trusted_connection=true'

    return connection_string


class Metric:
    """
    The base layer of the metrics framework. The Metric calls is created solely from the properties in metrics.yml.
    Input the name of the desired metric and the properties will be looked up form the YAML file.

    :param str input_name: name of the metric you would like to use
    """
    def __init__(self, input_name: str) -> Metric:
        # import YAML data as dictionary
        yml_path = path.join('..', 'metrics', 'metrics.yml')
        stream = open(yml_path, 'r')
        m = [met for met in yaml.unsafe_load(stream)['metrics'] if met['name'] == input_name][0]

        # assign Metric properties from yaml dict
        self.name = m['name']
        self.dimensions = m['dimensions']
        self.time_field = m['time_field']
        self.initial_sql = m.get('initial_sql')

        # create unique ID from md5 hash of name
        self.id = md5(str.encode(m['name'])).hexdigest()

        # parse sql and store as cte alised "bse" ready for aggregation and filtering
        base_sql = m['base_logic']
        sql_string = prep_sql(base_sql)
        self.sql = sql_string
        self.transform_sql = open(os.path.join('..', 'metrics', 'sql', base_sql), 'r').read()
        self.base_sql_file = base_sql
        self.base_parquet_file = path.join('base_sql_parquet', base_sql.replace('.sql', '.parquet'))

        # get python list of edw table dependencies needed in your data mart to use this metric
        runner = LineageRunner(self.sql)

        def dep_table_name(t):
            if str(t.schema) == Schema.unknown:
                return t.raw_name
            else:
                return f'{str(t.schema)}.{t.raw_name}'

        self.dependencies = list(set([dep_table_name(table) for table in runner.source_tables]))

        # set measure field and aggregation properties
        measure_dict = m['measure']
        measure_agg = measure_dict['aggregation']
        measure_field = measure_dict['field']

        # decode aggregation into t-sql aggregate function string and store as "measure" property
        if measure_agg == 'count distinct':
            measure_string = f'count(distinct "{measure_field}")'
        elif measure_agg == 'sum':
            measure_string = f'sum("{measure_field}")'
        elif measure_agg == 'maximum':
            measure_string = f'max("{measure_field}")'
        elif measure_agg == 'minimum':
            measure_string = f'min("{measure_field}")'
        elif measure_agg == 'average':
            measure_string = f'avg("{measure_field}")'
        else:
            measure_string = f"{measure_field}"

        self.measure = measure_string


class MetricInstance:
    """
    MetricInstances are specific permutations of Metrics and their available features.
    A metric instance takes in a metric, name for the instance, timeframe, and optional filter and group by logic.
    The result is a unique permutation of a Metric to be reported on or used as an input for an analysis.

    :param Metric metric: the base metric to aggregate and filter on
    :param str name: name of the metric instance, name should reflect the result of the aggregates and filters performed
    :param Timeframe timeframe: enumerated value representing the timeframe to aggregate to (i.e. daily, monthly, etc)
    :param list group_bys: python list of dimensions to group by (dimensions must be documented in metrics.yml)
    :param str filters: text string of valid sql where clause to filter on
    :param int start_date: date metric values begin, date as integer YYYYMMDD format
    :param int end_date: date metric values stop, date as integer YYYYMMDD format
    """
    def __init__(
            self, metric: Metric,
            name: str,
            timeframe: Timeframe,
            group_bys: list = [],
            filters: str = None,
            start_date: int = 20180101,
            end_date: int = int((date.today() - timedelta(days=1)).strftime('%Y%m%d'))
    ) -> MetricInstance:
        # make sure values passed in group_bys list are documented dimensions for this metric, throw error if not
        if group_bys:
            group_by_exceptions = set(group_bys) - set(metric.dimensions)
            valid_groupby = all(elem in metric.dimensions for elem in group_bys)
            if not valid_groupby:
                raise ValueError(
                    f'The following fields submitted to "group by" are not documented dimensions for {metric.name}: '
                    f'{linesep}{", ".join(group_by_exceptions)}'
                )

        # select through name, timeframe, and create a unique ID from base metric and inputs
        self.name = name
        self.timeframe = timeframe
        self.metric = metric

        if not group_bys:
            group_bys_sorted = []
        else:
            group_bys_sorted = group_bys.sort()

        self.id = md5(str.encode(str([metric, name, timeframe, group_bys_sorted, filters]))).hexdigest()

        # create strings of SQL snippits for time manipulation based on Timeframe attribute
        if self.timeframe == Timeframe.DAILY:
            timeframe_string = f'cast("{metric.time_field}" as date)'
            timeframe_string_select = 'spine."Date"'
            timeframe_spine_string = 'spine."Date"'
            timeframe_string_trans = f'cast("{metric.time_field}" as date)'
            timeframe_string_select_trans = 'spine."Date"'
            timeframe_spine_string_trans = 'spine."Date"'
        elif self.timeframe == Timeframe.WEEKLY:
            timeframe_string = f'dateadd(week, datediff(week, -1, "{metric.time_field}"), -1)'
            timeframe_string_select = 'dateadd(week, datediff(week, -1, spine."Date"), -1) as "Week"'
            timeframe_spine_string = 'dateadd(week, datediff(week, -1, spine."Date"), -1)'
            timeframe_string_trans = f'''date_trunc('week', "{metric.time_field}")'''
            timeframe_string_select_trans = '''date_trunc('week', spine."Date") as "Week"'''
            timeframe_spine_string_trans = '''date_trunc('week', spine."Date")'''
        elif self.timeframe == Timeframe.MONTHLY:
            timeframe_string = f'datefromparts(year("{metric.time_field}"), month("{metric.time_field}"), 1)'
            timeframe_string_select = 'datefromparts(year(spine."Date"), month(spine."Date"), 1) as "Month"'
            timeframe_spine_string = 'datefromparts(year(spine."Date"), month(spine."Date"), 1)'
            timeframe_string_trans = f'''date_trunc('month', "{metric.time_field}")'''
            timeframe_string_select_trans = '''date_trunc('month', spine."Date") as "Month"'''
            timeframe_spine_string_trans = '''date_trunc('month', spine."Date")'''
        else:
            timeframe_string = f'cast("{metric.time_field}" as date)'
            timeframe_string_select = 'spine."Date"'
            timeframe_spine_string = 'spine."Date"'
            timeframe_string_trans = f'cast("{metric.time_field}" as date)'
            timeframe_string_select_trans = 'spine."Date"'
            timeframe_spine_string_trans = 'spine."Date"'

        # create strings of SQL snippits for group bys, creates strings for select clause and group by clause
        if group_bys:
            group_bys_sanitized = [f'"{col}"' for col in group_bys]
            group_bys_sanitized_spine = [f'spine."{col}"' for col in group_bys]
            group_by_string = f', {", ".join(group_bys_sanitized)}'
            group_by_string_spine = f', {", ".join(group_bys_sanitized_spine)}'
            group_by_select_spine = ", ".join(group_bys_sanitized_spine) + ","
            group_by_join = f'{linesep}        and ' + f'{linesep}        and '.join([f'spine."{col}" = bse2."{col}"' for col in group_bys])
        else:
            group_by_string = ''
            group_by_join = ''
            group_by_string_spine = ''
            group_by_select_spine = ''

        def date_int_to_str(date_int):
            return f'cast(cast({date_int} as varchar) as date)'

        def date_to_str_trans(date_int):
            return f"strptime(cast({date_int} as varchar), '%Y%m%d')"

        date_filter_string = f'{timeframe_string} between {date_int_to_str(start_date)} and {date_int_to_str(end_date)}'
        date_filter_str_trans = f'{timeframe_string_trans} between {date_to_str_trans(start_date)} and {date_to_str_trans(end_date)}'

        if filters:
            filter_string = f'{linesep}    {filters} and {date_filter_string}'
            filter_string_trans = f'{linesep}    {filters} and {date_filter_str_trans}'
        else:
            filter_string = f'{linesep}    where {date_filter_string}'
            filter_string_trans = f'{linesep}    where {date_filter_str_trans}'

        # creates select string for all MetricInstance base properties: numeric value, base metric name, instance name
        metric_string = (
            f'''coalesce(cast({metric.measure} as float), 0) as "Value", '{metric.name}' as "Base Metric", '{self.name}' as "Metric Instance"'''
        )

        # put together final MetricInstance SQL query on top of Metric.sql bse alias
        sql_text = metric.sql + f''',

bse2 as (
    select *
    from bse{filter_string}
),

dte as (
    select
        DateValue as "Date"
    from "EDW_SEM"."dbo"."Dim_Date"
    where DateValue between {date_int_to_str(start_date)} and {date_int_to_str(end_date)}
),

spine as (
    select distinct
        "Date"{group_by_string}
    from bse2
    cross join dte
)

select
    '{self.id}' as "Metric ID",
    {timeframe_string_select},
    {group_by_select_spine}
    {metric_string},
    getdate() as "Metric Instance Run At"
from spine
left join bse2
    on spine."Date" = {timeframe_string}{group_by_join}
group by {timeframe_spine_string}{group_by_string_spine}

'''
        self.sql = sql_text

        sql_text_transform = f'''
            create table bse2 as
                select *
                from read_parquet('{self.metric.base_parquet_file}'){filter_string_trans};
            
            create table dte as
                select
                    DateValue as "Date"
                from read_parquet('{os.path.join('base_sql_parquet', 'date.parquet')}')
                where DateValue between {date_to_str_trans(start_date)} and {date_to_str_trans(end_date)};
            
            create table spine as 
                select distinct
                    "Date"{group_by_string}
                from bse2
                cross join dte;
            
            select
                '{self.id}' as "Metric ID",
                {timeframe_string_select_trans},
                {group_by_select_spine}
                {metric_string},
                now() as "Metric Instance Run At"
            from spine
            left join bse2
                on spine."Date" = {timeframe_string_trans}{group_by_join}
            group by {timeframe_spine_string_trans}{group_by_string_spine}
        '''

        self.transform_sql = sql_text_transform

        if metric.initial_sql:
            initial_sql_path = os.path.join('..', 'metrics', 'sql', 'initial_sql', metric.initial_sql)
            self.initial_sql = open(initial_sql_path, 'r').read()
        else:
            self.initial_sql = None

    # execute MetricInstance SQL query and return pandas Dataframe
    def to_pandas(self, data_mart: str) -> DataFrame:
        """
        Executes MetricInstance SQL query in EDW data mart specific in connection. Returns pandas Dataframe.

        :param data_mart: string representing data mart metrics are run from
        :return: pandas Dataframe of a MetricInstance
        """

        return self.to_arrow(data_mart).to_pandas()

    # execute MetricInstance SQL query and return Apache Arrow table
    def to_arrow(self, data_mart: str) -> Table:
        """
        Executes MetricInstance SQL query in EDW data mart specific in connection. Returns pyarrow Table.

        :param data_mart: string representing data mart metrics are run from
        :return: pyarrow Table of a MetricInstance
        """

        cx_conn = make_mssql_connection('connectorx', data_mart)

        src_pq_file = self.metric.base_parquet_file
        if os.path.exists(src_pq_file):
            print(f"Base SQL parquet file {src_pq_file} already exists!")
        else:
            print(f"Creating base SQL parquet file {src_pq_file}")

            if self.initial_sql:
                sa_conn = make_mssql_connection('sqlalchemy', data_mart)
                sa_engine = sa.create_engine(sa_conn)
                sa_engine.execution_options(autocommit=True).execute(self.initial_sql)

            write_table(cx.read_sql(cx_conn, self.metric.transform_sql, return_type='arrow2'), src_pq_file)

        duckdb_conn = duckdb.connect(database=':memory:')
        arrow_table = duckdb_conn.execute(self.transform_sql).arrow()
        duckdb_conn.close()

        return arrow_table

    # execute MetricInstance SQL query and output parquet file
    def to_parquet(self, data_mart: str, parquet_file_name: str):
        """
        Executes MetricInstance SQL query in EDW data mart specific in connection. Writes output to a parquet file.

        :param data_mart: string representing data mart metrics are run from
        :param parquet_file_name: name of target parquet file
        :return: no return value, parquet file created as result
        """
        arrow_table = self.to_arrow(data_mart)
        print(f'Writing "{self.name}" to parquet file: {parquet_file_name}.')
        write_table(arrow_table, parquet_file_name)

    # execute MetricInstance SQL query and output csv file
    def to_csv(self, data_mart: str, csv_file_name: str):
        """
        Executes MetricInstance SQL query in EDW data mart specific in connection. Writes output to a csv file.

        :param data_mart: string representing data mart metrics are run from
        :param csv_file_name: name of target csv file
        :return: no return value, csv file created as result
        """
        df = self.to_pandas(data_mart)
        print(f'Writing "{self.name}" to csv file: {csv_file_name}.')

        df.to_csv(csv_file_name, index=False)

    # execute MetricInstance SQL query and output Tableau Hyper file
    def to_hyper(self, data_mart: str, hyper_file_name: str = 'metric.hyper'):
        """
        Executes MetricInstance SQL query in EDW data mart specific in connection. Writes output to a hyper file.

        :param data_mart: string representing data mart metrics are run from
        :param hyper_file_name: name of target Tableau Hyper file
        :return: no return value, Tableau Hyper file created as result
        """
        tmp_pq = 'tmp.parquet'
        self.to_parquet(data_mart, tmp_pq)
        print(f'Writing "{self.name}" to hyper file: {hyper_file_name}.')
        with HyperProcess(
                telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                parameters={'default_database_version': '2', 'log_config': ''},
        ) as hyper:
            with Connection(hyper.endpoint, hyper_file_name, CreateMode.CREATE_AND_REPLACE) as connection:
                connection.catalog.create_schema_if_not_exists('metrics')
                hyper_table = TableName('metrics', self.name)
                cta_sql = f'create table {hyper_table} as select * from external({escape_string_literal(tmp_pq)});'
                connection.execute_command(cta_sql)

        print(f'Removing temporary parquet file: {tmp_pq}.')
        os.remove(tmp_pq)

    # execute MetricInstance SQL query and write to EDW table in specified data mart
    def to_sql(self, data_mart: str, target_table_name: str, target_data_mart: str):
        """
        Executes MetricInstance SQL query in EDW data mart specific in connection. Writes output to a table in the EDW.

        :param data_mart: string representing data mart metrics are run from
        :param target_table_name: name of target EDW table
        :param target_data_mart: string representing target database name
        :return: no return value, table written to EDW as result
        """
        sa_conn = make_mssql_connection('sqlalchemy', target_data_mart)
        sa_engine = sa.create_engine(sa_conn, fast_executemany=True)
        df = self.to_pandas(data_mart)
        print(f'Writing "{self.name}" to SQL Server table: {target_table_name}.')
        df.to_sql(target_table_name, sa_engine, index=False, if_exists='replace')


class MetricsCollection:
    """
    MetricsCollections are a combined table that unions together all MetricInstances passed as a python list.
    If MetricInstances passed have differing schemas, these are handled one of two ways:
        - require_uniform_columns=False (default):
            columns are created for every column in every MetricInstance passed, not applicable columns populate as NULL
        - require_uniform_columns=True (optional):
            if the columns are not the same in each MetricInstance, fail and an error is thrown

    :param list metric_instance_ls: python list of MetricInstances to be unioned together
    :param bool require_uniform_columns: True/False flag for how to combine MetricInstances with differing schemas
    """
    def __init__(self, metric_instance_ls, require_uniform_columns=False):
        # set properties for list of MetricInstances, the list of MetricInstance names, and require_uniform_columns
        self.metric_instance_list = metric_instance_ls
        self.metric_instance_name_list = [mi.name for mi in self.metric_instance_list]
        self.uniform_columns_required = require_uniform_columns

        # get a distinct list of timeframes from the MetricInstances passed, raise an error if not all the same
        timeframe_set = set([str(mi.timeframe) for mi in self.metric_instance_list])
        timeframe_count = len(timeframe_set)
        timeframe_string = ', '.join(timeframe_set)

        if len(set([mi.timeframe for mi in self.metric_instance_list])) > 1:
            raise ValueError(
                f'All metric instances in a collection must have the same timeframe. This collection has '
                f'{timeframe_count}: {timeframe_string}'
            )

    # union all metric instances into single pyarrow Table
    def to_arrow(self, data_mart: str) -> Table:
        """
        pass in connection, return arrow table

        :param str data_mart: string representing data mart metrics are run from
        :return: pyarrow Table of a MetricsCollection
        """
        arrow_table_ls = []
        for mi in self.metric_instance_list:
            table = mi.to_arrow(data_mart)
            arrow_table_ls.append(table)

        print(f'Combining metrics into single metrics collection table in memory.')

        # handle varying input schemas according to uniform_columns_required parameter passed
        if self.uniform_columns_required:
            return concat_tables(arrow_table_ls, promote=False)
        else:
            return concat_tables(arrow_table_ls, promote=True)

    # union all metric instances into single pyarrow Table then write Table to parquet file
    def to_parquet(self, data_mart: str, parquet_file_name: str):
        """
        Executes all MetricInstance SQL queries in list from within the EDW data mart specified in "mssql_connection".
        Writes unioned output to a parquet file named by the "parquet_file_name" parameter.

        :param str data_mart: string representing data mart metrics are run from
        :param parquet_file_name: name of target parquet file
        :return: no return value, parquet file created as result
        """
        t = self.to_arrow(data_mart)
        print(f'Writing collection to parquet file: {parquet_file_name}.')
        write_table(t, parquet_file_name)

    # union all metric instances into single pyarrow Table then write Table to Tableau Hyper file
    def to_hyper(self, data_mart: str, hyper_file_name: str):
        """
        Executes all MetricInstance SQL queries in list from within the EDW data mart specified in "mssql_connection".
        Writes unioned output to a Tableau Hyper file named by the "hyper_file_name" parameter.

        :param str data_mart: string representing data mart metrics are run from
        :param hyper_file_name: name of target Tableau Hyper file
        :return: no return value, Tableau Hyper file created as result
        """

        # write arrow table to temporary parquet file
        tmp_pq = 'tmp.parquet'
        self.to_parquet(data_mart, tmp_pq)
        print(f'Writing collection to hyper file: {hyper_file_name}.')

        # start tableau hyper engine and define target hyper file location
        with HyperProcess(
                telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                parameters={'default_database_version': '1', 'log_config': ''},
        ) as hyper:
            with Connection(hyper.endpoint, hyper_file_name, CreateMode.CREATE_AND_REPLACE) as connection:
                # create target hyper schema and table
                connection.catalog.create_schema_if_not_exists('metrics')
                hyper_table = TableName('metrics', 'metrics_collection')

                # use create table as syntax and hyper's parquet schema inference to create target hyper table
                cta_sql = f'create table {hyper_table} as select * from external({escape_string_literal(tmp_pq)});'
                connection.execute_command(cta_sql)

        # remove temporary intermediate parquet file
        print(f'Removing temporary parquet file: {tmp_pq}.')
        os.remove(tmp_pq)

    # union all metric instances into single pyarrow Table then write Table to csv file
    def to_csv(self, data_mart: str, csv_file_name: str):
        """
        Executes all MetricInstance SQL queries in list from within the EDW data mart specified in "mssql_connection".
        Writes unioned output to a csv file named by the "csv_file_name" parameter.

        :param str data_mart: string representing data mart metrics are run from
        :param csv_file_name: name of target csv file
        :return: no return value, csv file created as result
        """
        t = self.to_arrow(data_mart)
        print(f'Writing collection to csv file: {csv_file_name}.')
        write_csv(t, csv_file_name)

    # union all metric instances into single pyarrow Table then transform to a pandas DataFrame to return
    def to_pandas(self, data_mart: str) -> DataFrame:
        """
        Executes all MetricInstance SQL queries in list from within the EDW data mart specified in "mssql_connection".
        Returns unioned output as a pandas DataFrame.

        :param str data_mart: string representing data mart metrics are run from
        :return: pandas DataFrame of a MetricsCollection
        """
        t = self.to_arrow(data_mart)
        return t.to_pandas()

    # union all metric instances into single pyarrow Table then transform to a pandas and write to EDW table
    def to_sql(self, data_mart: str, target_table_name: str, target_data_mart: str):
        """
        Executes all MetricInstance SQL queries in list from within the EDW data mart specified in "mssql_connection".
        Writes output to a table in the EDW within the target connection named by the "target_table_name" parameter.
        Current logic raises an error and fails if specified table already exists.

        :param str data_mart: string representing data mart metrics are run from
        :param str target_table_name: name of target EDW table
        :param str target_data_mart: string representing target data mart
        :return: no return value, table written to EDW as result
        """
        sa_conn = make_mssql_connection('sqlalchemy', target_data_mart)
        sa_engine = sa.create_engine(sa_conn, fast_executemany=True)
        df = self.to_pandas(data_mart)
        print(f'Writing to SQL Server table: {target_table_name}.')
        df.to_sql(target_table_name, sa_engine, index=False, if_exists='replace')