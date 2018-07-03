from sqlalchemy import create_engine
import pandas.io.sql as psql
import psycopg2

def get_engine_json( json_config ):
    engine = create_engine('postgresql://'+json_config['user']+':'+json_config['pwd']+'@'+json_config['host']+':'+json_config['port']+'/'+json_config['database']+'')
    engine.execution_options(autocommit=True, autoflush=False, expire_on_commit=False)
    c = engine.connect()
    return c

def get_engine( user, pwd, host, port, database ):
    engine = create_engine('postgresql://'+user+':'+pwd+'@'+host+':'+port+'/'+database+'')
    engine.execution_options(autocommit=True, autoflush=False, expire_on_commit=False)
    c = engine.connect()
    return c


def get_bounding_box( connection, table, SRID=4326 ):

    return psql.read_sql( """SELECT ST_SetSRID(ST_Extent(geom),"""+str(SRID)+""") as bextent FROM """+table+""";""", connection )

def get_table_srid( connection, schema, table_name ):
    
    sql = """SELECT Find_SRID("""+schema+""", """+table_name+""", 'geom')"""
    result_set = connection.execute(sql).fetchone()
    srid = None
    for v in result_set :
        srid = v
    
    return srid

def latitude_longitude_to_geom( connection, latitude, longitude, srid ):
    sql = """SELECT ST_SetSRID(ST_Point("""+str(longitude)+""", """+str(latitude)+"""),"""+str(srid)+""")"""
    result_set = connection.execute(sql).fetchone()
    geom = None
    for v in result_set :
        geom = v

    return geom

def store_dataframe( connection, dataframe, in_schema, table_name, geometry_type, srid, index=True, index_label=None ):
    
    dataframe.to_sql(table_name, connection, schema=in_schema,if_exists='replace', index= index, index_label = index_label )
    sql = """ALTER TABLE """+in_schema+"""."""+table_name+""" ALTER COLUMN geom TYPE Geometry("""+geometry_type+""", """+str(srid)+""") USING ST_SetSRID(geom::Geometry, """+str(srid)+""")"""
    connection.execute(sql)

def list_tables( database_url ):
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    print (cursor.fetchall() )