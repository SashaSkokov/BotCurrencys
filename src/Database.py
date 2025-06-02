import asyncpg
import json
import os
from contextlib import asynccontextmanager

class Config:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.json')

        try:
            with open(config_path, 'r') as config_file:
                self.data = json.load(config_file)
        except FileNotFoundError:
            raise Exception("Файл конфигурации не найден")
        except json.JSONDecodeError:
            raise Exception("Ошибка при парсинге JSON файла конфигурации")

    def get(self, key):
        return self.data.get(key)


# Создаем экземпляр конфигурации
config = Config()

pool = None


async def create_pool():
    global pool
    pool = await asyncpg.create_pool(
        database=config.get('database').get('name'),
        user=config.get('database').get('user'),
        password=config.get('database').get('password'),
        host=config.get('database').get('host'),
        max_size=10,
        min_size=1
    )
    await create_tables()
    return pool

async def create_tables():
    async with pool.acquire() as connection:
        await connection.execute('''
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE,
            username TEXT COLLATE "ru_RU.utf8",
            phone_number TEXT UNIQUE
        )
        ''')

        await connection.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
                                                     id SERIAL PRIMARY KEY,
                                                     user_id BIGINT UNIQUE,
                                                     username TEXT COLLATE "ru_RU.utf8",
                                                     currencies TEXT[],
                                                     is_active BOOLEAN DEFAULT TRUE,
                                                     subscribe_date TIMESTAMP DEFAULT NOW()
        )
        ''')

@asynccontextmanager
async def get_connection():
    if pool is None:
        await create_pool()
    connection = await pool.acquire()
    try:
        yield connection
    finally:
        await connection.close()