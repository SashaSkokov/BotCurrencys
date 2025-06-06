## Telegram Currency Bot

## Описание
Бот для получения курсов валют с возможностью подписки на рассылку.

## Функциональность
- Получение курсов USD и EUR
- Подписка на рассылку курсов
- Экспорт данных (для администратора)
- Управление подписками
## Требования
- Python 3.11+
- PostgreSQL
- Docker (для развертывания)
## Установка
1. Установите Python и pip
2. Установите зависимости:
- pip install -r requirements.txt
3. Настройте config.json
4. Запустите бота:
- python bot main.py
## Развертывание через Docker
1. Установите Docker и Docker Compose
2. Скопируйте пример конфигурации:
- cp config.example.json config.json
3. Настройте параметры в config.json
4. Запустите:
- docker-compose up --build

## Docker Compose
В проекте есть готовый файл docker-compose.yml, который позволяет развернуть все сервисы:

- Telegram бот
- PostgreSQL базу данных

## Команды бота
- /start - Начало
- /admin - Авторизация
- /usd - Курс доллара
- /eur - Курс евро
- /help - Помощь
- /subscribe - подписаться на рассылку
- /unsubscribe - отписаться от рассылки
- /export - Экспорт данных (для администратора)
- /activeSubscribers - Посмотреть активных подписчиков (для администратора)

## Безопасность
- Валидация входных данных при экспорте
- Конфиденциальные данные хранятся в отдельном файле
- Доступ к базе данных защищен
- Защита от SQL-инъекций
- Защита от переполнения буфера
- Ограничение на количество запросов
- Защита от XSS-атак
- Безопасное удаление файлов

## Поддержка
При возникновении вопросов или проблем, обращайтесь к разработчикам.