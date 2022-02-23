
work_dir = '/tmp/tdx/'


class QuestConfig:
    user = "admin"
    password = "quest"
    host = "127.0.0.1"
    port = "8812"
    rest_port = "9000"
    database = "qdb"
    table = "trade"
    dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(
        user=user,
        password=password,
        host=host,
        port=port,
        db=database
    )


quest = QuestConfig()
