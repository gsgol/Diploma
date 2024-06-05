import requests
import json
import os
from openai import OpenAI
import psycopg2
from tabulate import tabulate
from aiogram import Bot
from aiogram.enums.parse_mode import ParseMode
import asyncio
import pandas as pd
import Diplomabot.config

databases = Diplomabot.config.databases
BOT_TOKEN = Diplomabot.config.BOT_TOKEN

bot = Bot(token=BOT_TOKEN)
selected_base = "temp"


connect = psycopg2.connect(
    user="temp", password="1234", host="127.0.0.1", port="5433", dbname="postgres_db"
)


connection = False
client = OpenAI(
    api_key=Diplomabot.config.API_KEY, base_url="https://api.proxyapi.ru/openai/v1"
)


def openAI(prompt):
    completion = client.chat.completions.create(
        model=Diplomabot.config.MODEL,
        messages=[{"role": "assistant", "content": f"{prompt}"}],
        temperature=0,
    )
    return completion.choices[0].message.content


async def telegram_bot_sendtable(bot_message, chat_id):
    await bot.send_message(chat_id=chat_id, text=bot_message, parse_mode=ParseMode.HTML)
    return 0


async def telegram_bot_sendmessage(bot_message, chat_id):
    await bot.send_message(chat_id=chat_id, text=bot_message)
    return 0


async def get_info_from_database(connection, result, chat_id):
    if connection:
        base = f"""структура базы {databases[selected_base]} """
        cursor = connect.cursor()
        prompt = result["message"]["text"].replace(
            "/ask",
            f"создай запрос на языке PostgreSQL для базы данных {base} для ответа на вопрос ",
        )
        bot_response = openAI(f"{prompt}").replace("\n", " ")
        names = ""
        try:
            cursor.execute(f"{bot_response}")
            for i in cursor.description:
                names += i.name + " "
            resulted_names = []
            resulted_names.append(names.split())
            resulted_names.extend(cursor.fetchall())
            resulted_names = tabulate(resulted_names, tablefmt="pipe")
            resulted_names = "<pre>\n" + resulted_names + "\n</pre>"
            await telegram_bot_sendtable(resulted_names, chat_id)
        except:
            print(bot_response)
            await telegram_bot_sendmessage("Неверный запрос", chat_id)
        finally:
            cursor.close()
    else:
        await telegram_bot_sendmessage(
            "Вы не подключены ни к одной базе данных", chat_id
        )


async def login(result, chat_id):
    global connect
    global selected_base
    info = result["message"]["text"].replace("/login ", "").split(" ")
    user, password, dbname = info[0], info[1], info[2]
    try:
        connect = psycopg2.connect(
            user=f"{user}",
            password=f"{password}",
            host="127.0.0.1",
            port="5433",
            dbname=f"{dbname}",
        )
        selected_base = f"{dbname}"
        await telegram_bot_sendmessage("Подключение произошло успешно", chat_id)
        return "connected"
    except Exception as e:
        await telegram_bot_sendmessage("Ошибка подключения", chat_id)
        return "error"


async def insert_data_from_file(connection, result, chat_id):
    global connect
    if connection:
        cursor = connect.cursor()
        table = result["message"]["caption"]
        try:
            file_info = await bot.get_file(result["message"]["document"]["file_id"])
            filepath = os.getcwd() + "\\new_file.xlsx"
            downloaded_file = await bot.download_file(file_info.file_path, filepath)
            temp = pd.read_excel(filepath)
            stringus = temp.to_string(index=False)
            mod_stringus = (
                f"создай запрос для таблицы {table} для добавления данных {stringus}"
            )
            query = openAI(f"{mod_stringus}")
            cursor.execute(query)
            await telegram_bot_sendmessage("Данные записаны", chat_id)
        except:
            await telegram_bot_sendmessage("Неверные данные", chat_id)
        finally:
            connect.commit()
    else:
        await telegram_bot_sendmessage(
            "Вы не подключены ни к одно базе данных", chat_id
        )


async def Chatbot(connection):
    global connect
    global selected_base
    cwd = os.getcwd()
    filename = cwd + "/chatgpt.txt"
    if not os.path.exists(filename):
        with open(filename, "w") as f:
            f.write("1")
    else:
        print("File Exists")

    with open(filename) as f:
        last_update = f.read()

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={last_update}"
    response = requests.get(url)
    data = json.loads(response.content)

    for result in data["result"]:
        try:
            if float(result["update_id"]) > float(last_update):

                last_update = str(int(result["update_id"]))
                chat_id = str(result["message"]["chat"]["id"])
                with open(filename, "w") as f:
                    f.write(last_update)

                if "document" not in result["message"]:
                    if "/ask" in result["message"]["text"]:
                        await get_info_from_database(connection, result, chat_id)

                    if "/login" in result["message"]["text"]:
                        return await login(result, chat_id)

                    if ("/help" or "/start") in result["message"]["text"]:
                        await telegram_bot_sendmessage(
                            """Добро пожаловать в бот помощник для SQL запросов\nСписок комманд бота:\n/ask - команда для запроса в базу данных\n/login  - команда для подключения к определенной базе данных (формат ввода /login логин пароль база данных)\n/end - команда для завершения рабочей сессии\n/help - команда для вывода сообщения со справочной информацией\nТакже можно отправить файл формата xlsx с данными и именем таблицы в которую записать данные""",
                            chat_id,
                        )
                    if "/end" in result["message"]["text"]:

                        connect.commit()
                        connect.close()
                        await telegram_bot_sendmessage("Сессия завершена", chat_id)

                        return "disconnected"

                if "document" in result["message"]:
                    await insert_data_from_file(connection, result, chat_id)

        except Exception as e:
            print(e)
            await telegram_bot_sendmessage("Произошла ошибка\n", chat_id)

    return "done"


async def main():
    global connection
    while True:
        timertime = 5
        staus = await Chatbot(connection)
        if staus == "connected":
            connection = True
        if staus == "disconnected":
            connection = False
        await asyncio.sleep(timertime)


if __name__ == "__main__":
    asyncio.run(main())
