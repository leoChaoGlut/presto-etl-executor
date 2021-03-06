import json
from sys import argv

import prestodb
import pymysql
import requests

"""
Usage:
python3 ${thisScript} presto.host=${} presto.port=${} presto.user=${} ...
具体参数列表请看 NECESSARY_PARAMS 和 OPTIONAL_PARAMS
"""


def getMySQLConnection():
    return pymysql.connect(
        host='localhost',
        port=3306,
        user='etl',
        password='etl',
        db='etl',
        charset='utf8',
        cursorclass=pymysql.cursors.DictCursor
    )


NECESSARY_PARAMS = {
    'presto.host': {
        'example': 'localhost'
    },
    'presto.port': {
        'example': '8080'
    },
    'presto.user': {
        'example': 'dev'
    },
    'presto.catalog': {
        'example': 'dev_hive'
    },
    'presto.schema': {
        'example': 'ods_hive'
    },
    'sql.url.prefix': {
        'example': 'http://localhost',
        'comment': '提供 sql 文件的远程目录.可用 apache ,nginx 等文件服务器提供 sql 文件访问功能.'
    },
    'sql.names': {
        'format': 'sqlName1,sqlName2,sqlName3',
        'example': 'fully,increasingly,parly',
        'comment': '逗号分隔每一个 sql 文件名,这些 sql 将会被顺序执行'
    },
}
OPTIONAL_PARAMS = {
    'placeholder.sql': {
        'example': 'placeholder',
        'comment': 'placeholder sql 文件名,执行后,可以获得所有 placeholder, 用于填充 sql.names 的 sql'
    },
    'placeholder.loop': {
        'example': 'p_create_time:party',
        'format': 'placeholder1:sqlName1,placeholder2:sqlName2',
        'comment': '通过一个以逗号分隔的 placeholderValue.split(","),决定一个 sql 脚本循环的次数'
    },
    'placeholder.loop.value.separator': {
        'comment': 'placeholder value 的分隔符,默认是逗号',
        'relation': 'placeholder.loop'
    },
    'placeholder.save': {
        'example': 'max_create_time,max_audit_time,max_id',
        'format': 'placeholder1,placeholder2,placeholder3',
        'comment': '需要被保存的 placeholder 名,逗号分隔'
    },
    'placeholder.save.id': {
        'comment': '1.需要保存的 placeholder 的数据库唯一 id, 通常用 schema.table 来作为id. 2.关联 placeholder.save,当 placeholder.save 有值, placeholder.save.id 也必须有值',
        'example': 'clerk_info',
        'relation': 'placeholder.save.id'
    },
}

DEFAULT_LOOP_VALUE_SEPARATOR = ','


def parseParams():
    params = {}
    i = 1
    while i < len(argv):
        kv = argv[i].split('=', 1)
        params[kv[0]] = kv[1]
        i += 1
    return params


def checkNecessaryParams(params):
    for np in NECESSARY_PARAMS.keys():
        if np in params.keys():
            npValue = params[np].strip()
            if not npValue:
                raise Exception("Necessary Param's Value Should Not Be Empty: " + np)
        else:
            raise Exception("Necessary Param Not Found: " + np)

    if 'placeholder.save' in params.keys():
        if 'placeholder.save.id' not in params.keys():
            raise Exception("When placeholder.save is provided,placeholder.save.id must provided too.")


def getPrestoConnection(params):
    return prestodb.dbapi.connect(
        host=params['presto.host'],
        port=params['presto.port'],
        user=params['presto.user'],
        catalog=params['presto.catalog'],
        schema=params['presto.schema'],
    )


def getSQL(url):
    print(url)
    resp = requests.get(url)
    print(resp)
    if resp.status_code == 200:
        return resp.text
    else:
        raise Exception(str(resp.status_code) + ',' + resp.reason + ': ' + url)


# 需要由调用方保证 :
# 1. sql 是可执行的
# 2. 调用完方法后,如果需要获取 resultSet 需要再调用 fetch
def execSQL(prestoCur, sql):
    print(sql)
    prestoCur.execute(sql)


def getSQLFiles(params):
    sqlFiles = []
    sqlNames = params['sql.names'].split(',')
    sqlUrlPrefix = params['sql.url.prefix']
    for sqlName in sqlNames:
        sqlFiles.append([sqlName, getSQL(sqlUrlPrefix + '/' + sqlName + '.sql')])
    return sqlFiles


def fillPlaceholder(sqlFile, placeholders):
    for key in placeholders:
        sqlFile = sqlFile.replace('{' + key + '}', str(placeholders[key]))
    return sqlFile


def execSQLFileIgnoreResult(prestoCur, sqlFile):
    sqls = sqlFile.split(";")
    if sqls:
        for sql in sqls:
            sql = sql.strip()
            if sql:
                execSQL(prestoCur, sql)
                print(prestoCur.fetchall())


def getPlaceholders(prestoCur, params):
    placeholders = {}
    if 'placeholder.sql' in params.keys():
        placeholderSql = params['placeholder.sql'].strip()
        if placeholderSql:
            sqlUrlPrefix = params['sql.url.prefix']
            sqlFile = getSQL(sqlUrlPrefix + '/' + placeholderSql + '.sql')
            sqls = sqlFile.split(";")
            if sqls:
                for sql in sqls:
                    sql = sql.strip()
                    if sql:
                        execSQL(prestoCur, sql)
                        result = prestoCur.fetchone()
                        print(result)
                        if result:
                            i = 0
                            while i < len(result):
                                placeholders[result[i]] = result[i + 1]
                                i += 2
    print(placeholders)
    return placeholders


def savePlaceholders(params, placeholders):
    phToBeSaved = {}
    if 'placeholder.save' in params.keys() and placeholders:
        placeholderSave = params['placeholder.save']
        if placeholderSave:
            placeholderSave = placeholderSave.split(',')
            for ps in placeholderSave:
                if ps in placeholders.keys():
                    phToBeSaved[ps] = placeholders[ps];
    print(phToBeSaved)
    if phToBeSaved:
        phToBeSavedJSON = json.dumps(phToBeSaved)
        mysqlConn = getMySQLConnection()
        try:
            with mysqlConn.cursor() as cursor:
                sql = """
                    INSERT INTO placeholder_record 
                    VALUES ('{}','{}')
                    ON DUPLICATE KEY UPDATE placeholders = '{}'
                """.format(
                    params['placeholder.save.id'],
                    phToBeSavedJSON,
                    phToBeSavedJSON,
                )
                print(sql)
                cursor.execute(sql)
                print(cursor.fetchall())
            mysqlConn.commit()
        finally:
            mysqlConn.close()


def getLoopParam(params):
    loopParam = {}
    if 'placeholder.loop' in params.keys():
        pl = params['placeholder.loop'].strip()
        if pl:
            phSqls = pl.split(',')
            for phSql in phSqls:
                split = phSql.split(':')
                placeholder = split[0]
                sqlName = split[1]
                loopParam[sqlName] = placeholder
    return loopParam


def getLoopValueSeparator(params):
    if 'placeholder.loop.value.separator' in params.keys():
        return params['placeholder.loop.value.separator']
    else:
        return DEFAULT_LOOP_VALUE_SEPARATOR


def loopSql(prestoCur, sqlFile, placeholders, loopPlaceholderKey, params):
    loopValueSeparator = getLoopValueSeparator(params)
    loopValues = placeholders[loopPlaceholderKey].split(loopValueSeparator)
    for key in placeholders:
        if key != loopPlaceholderKey:
            sqlFile = sqlFile.replace('{' + key + '}', placeholders[key])
    for lv in loopValues:
        tempSqlFile = sqlFile.replace('{' + loopPlaceholderKey + '}', lv)
        execSQLFileIgnoreResult(prestoCur, tempSqlFile)


def exec():
    params = parseParams()
    checkNecessaryParams(params);
    with getPrestoConnection(params) as prestoConn:
        prestoCur = prestoConn.cursor()
        placeholders = getPlaceholders(prestoCur, params)
        loopParam = getLoopParam(params)

        sqlFiles = getSQLFiles(params)
        for pair in sqlFiles:
            sqlName = pair[0]
            sqlFile = pair[1]
            print(sqlName)
            if sqlName in loopParam.keys():
                loopSql(prestoCur, sqlFile, placeholders, loopParam[sqlName], params)
            else:
                execSQLFileIgnoreResult(prestoCur, fillPlaceholder(sqlFile, placeholders))

        savePlaceholders(params, placeholders)


if __name__ == '__main__':
    exec()
