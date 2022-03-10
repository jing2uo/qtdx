
work_dir = '/tmp/tdx/'


class QuestDBConfig:
    user = "admin"
    password = "quest"
    host = "127.0.0.1"
    port = "8812"
    rest_port = "9000"
    database = "qdb"
    dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(
        user=user,
        password=password,
        host=host,
        port=port,
        db=database
    )


quest = QuestDBConfig()


sh_history = 'https://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip'
sz_history = 'https://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip'
