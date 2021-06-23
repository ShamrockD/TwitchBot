import config

import socket
import random
import datetime
import ssl
import csv
import time
import os.path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd

from collections import namedtuple

import threading

TEMPLATE_COMMANDS = {
    '!discord': 'Присоединяйся к дискорду {message.channel} , {message.user}',
    '!so': 'Зацени {message.text_args[0]}, он охуителен!!!',
}

Message = namedtuple(
    'Message',
    'prefix user channel irc_command irc_args text text_command text_args',
)

Bettimeout = 0  # 1 minute from now

seconds_for_timer = config.TIMER_FOR_VOTING
multiplier_for_timer = 1

allowed_users = list(config.STREAM_ALLOWED_USERS)


class Bot:
    timer = float()

    def __init__(self):
        self.irc_server = 'irc.chat.twitch.tv'
        self.irc_port = 6697
        self.oauth_token = config.OAUTH_TOKEN
        self.username = config.BOT_USERNAME  # Сюда пишем имя бота
        self.channels = [config.STREAM_CHANNEL_NAME]  # Сюда пишем название нужного канала
        self.custom_commands = {
            '!date': self.reply_with_date,
            '!ping': self.reply_to_ping,
            '!chance': self.reply_with_randint,
            '!dice': self.dice_it,
        }
        self.bet_start = {
            '!betstart': self.begin_betting,
        }
        self.bet_stops = {
            # '!betstop': self.bet_stopping,
            '!betstop',
        }
        self.bet_command = {
            '!win',
            '!lose',
        }


        self.data_pandas = []

        # Проверка даты
        checktime = time.localtime()
        self.checktime = f'{checktime.tm_mday}.{checktime.tm_mon}.{checktime.tm_year}'

        self.votedusers = []  # Список юзверей участвовавших в голосовании
        self.voting = []
        self.votes_headers = ['Id пользователя', 'Вариант', 'Результат', self.checktime]

        self.timer = 0

    def send_privmsg(self, channel, text):
        self.send_command(f'PRIVMSG #{channel} : {text}')

    def send_command(self, command):
        if 'PASS' not in command:
            print(f'< {command}')
        self.irc.send((command + '\r\n').encode())

    def connect(self):
        self.irc = ssl.wrap_socket(socket.socket())
        self.irc.connect((self.irc_server, self.irc_port))
        self.send_command(f'PASS {self.oauth_token}')
        self.send_command(f'NICK {self.username}')
        for channel in self.channels:
            self.send_command(f'JOIN #{channel}')
            self.send_privmsg(channel, 'Бот подключен, добро пожаловать.')

        self.loop_for_msgs()

    def get_user_from_prefix(self, prefix):
        domain = prefix.split('!')[0]
        if domain.endswith('.tmi.twitch.tv'):
            return domain.replace('.tmi.twitch.tv', '')
        if '.tmi.twitch.tv' not in domain:
            return domain
        return None

    def parse_message(self, received_msg):
        parts = received_msg.split(' ')

        prefix = None
        user = None
        channel = None
        text = None
        text_command = None
        text_args = None
        irc_command = None
        irc_args = None

        if parts[0].startswith(':'):
            prefix = parts[0][1:]
            user = self.get_user_from_prefix(prefix)
            parts = parts[1:]

        text_start = next(
            (idx for idx, part in enumerate(parts) if part.startswith(':')),
            None
        )

        if text_start is not None:
            text_parts = parts[text_start:]
            text_parts[0] = text_parts[0][1:]
            text = ''.join(text_parts)
            text_command = text_parts[0]
            text_args = text_parts[1:]
            parts = parts[:text_start]

        irc_command = parts[0]
        irc_args = parts[1:]

        hash_start = next(
            (idx for idx, part in enumerate(irc_args) if part.startswith('#')),
            None
        )
        if hash_start is not None:
            channel = irc_args[hash_start][1:]

        message = Message(
            prefix=prefix,
            user=user,
            channel=channel,
            text=text,
            text_command=text_command,
            text_args=text_args,
            irc_command=irc_command,
            irc_args=irc_args

        )
        # print(message)
        return message

    def thread_test_printer(self):
        # print('Test Passed')
        # time.sleep(1)
        # print('1 Second remain')
        # time.sleep(1)
        pass

    def handle_template_command(self, message, text_command, template):
        text = template.format(**{
            'message': message
        })
        self.send_privmsg(message.channel, text)

    def reply_with_date(self, message):
        formatted_date = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        text = f'Да пжлста @{message.user}, местное время: {formatted_date}.'
        self.send_privmsg(message.channel, text)

    def reply_to_ping(self, message):
        text = f'Hey @{message.user}, Чики-пики, Понг!'
        self.send_privmsg(message.channel, text)

    def dice_it(self, message):
        text = f'@{message.user} Дайс показал {random.randint(1, 6)}!'
        self.send_privmsg(message.channel, text)

    def reply_with_randint(self, message):
        text = f'По версии синоптиков шансы на победу {random.randint(0, 100)}. '
        self.send_privmsg(message.channel, text)

    def bet_check_win(self, message):
        if time.time() < self.timer and (
                message.text == '!win' or message.text == '!lose') and message.user not in self.votedusers:
            # with open('logger.csv', 'w', encoding='utf8', errors='replace', newline='\n') as file:
                if message.user not in self.votedusers:
                    self.votedusers.append(message.user)
                    # headwriter=csv.DictWriter(file, fieldnames=self.votes_headers)
                    # headwriter.writeheader()
                    slovar = ({
                        'id': message.user,
                        'text': message.text,
                        'date': self.checktime
                    })
                    self.voting.append([slovar['id'], slovar['text'], slovar['date']])
                    self.bet_dataframe_checker()

                    print('---------------------------', self.voting)
                    # writer = csv.writer(file, delimiter=";")
                    # writer.writerows(self.voting)

                    print('__________________________________________')


                else:
                    # text = f' {message.user}  Ты уже голосовал'
                    # self.send_privmsg(message.channel, text)
                    pass
                    # print(f'{message.user} Уже голосовал')

        elif time.time() < self.timer:
            text = f' @{message.user}  Твое участие в голосовании уже подтверждено'
            self.send_privmsg(message.channel, text)

            print(f'{message.user} Users voice already approved')

        elif time.time() > self.timer:
            text = f' @{message.user}  Сейчас нет голосования'
            self.send_privmsg(message.channel, text)

        else:
            print('All is done properly')

    def bet_dataframe_checker(self):
        try:
            if os.path.exists('CashDataFrame.csv'):

                df = pd.read_csv('CashDataFrame.csv').reset_index(drop=True)
                print('Файл датафрэйма существует')
                print('Here is dataframe from existed file', '\n', df)

                df1 = pd.DataFrame(self.voting, columns=['UserID','Choice','Date'])
                print('>>>')
                print(df1)

                df = df.append(df1) #.reset_index(drop=True)
                print('>>>')
                print(df)

                df.to_csv('CashDataFrame.csv', encoding='utf8', index=False)
                print('Data Frame correctly writed')

            else:

                print('Файл не найден, пришлось создать его заново')
                df = pd.DataFrame(self.voting, columns=['UserID','Choice','Date'])
                df.to_csv('CashDataFrame.csv', encoding='utf8', index=False)

                print(df.columns)

        finally:
            print('bet data frame completed ends Finally')

    def final_score_public(self):
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

        client_secret_json = config.SPREAD_SHEETS_DIRECTORY

        credentials = ServiceAccountCredentials.from_json_keyfile_name(client_secret_json, scope)
        spread_sheets_client = gspread.authorize(credentials)

        # Name of the spreadsheet created in google drive
        spreadsheets_url = config.GOOGLE_SHEETS_FILE_URL


        spreadsheet = spread_sheets_client.open_by_url(spreadsheets_url)

        with open('FinalScores.csv', 'r') as file_obj:
            content = file_obj.read()
            spread_sheets_client.import_csv(spreadsheet.id, data=content)

    def bet_stopping(self, message):
        text = f'Голосование окончено, результаты больше не принимаются'
        self.send_privmsg(message.channel, text)
        self.voting.clear()
        self.votedusers.clear()

    def betting_stop_check(self, message):
        self.timer = (self.timer - seconds_for_timer * multiplier_for_timer)
        print(message.text_args)
        data_frame_score_values = pd.read_csv('CashDataFrame.csv').reset_index(drop=True)
        if message.text_args == ['win'] or message.text_args == []:

            print('Остановка голосования, засчитана Победа')
            data_frame_score_values.replace(['!win', '!lose'], [10, 0], inplace=True)
            self.bet_stopping(message)

            print(data_frame_score_values, '>>>>>>>>>Это результаты победы')
            data_frame_score_values.to_csv('Scores.csv', encoding='utf8', index=False)

            df = data_frame_score_values.groupby(['UserID', 'Date']).sum()[['Choice']]

            print(df)

            df.to_csv('FinalScores.csv', encoding='utf8', index=True)

            self.final_score_public()

        elif message.text_args == ['lose']:

            print('Остановка голосования, засчитано Поражение')
            data_frame_score_values.replace(['!win', '!lose'], [0, 10], inplace=True)
            self.bet_stopping(message)

            print(data_frame_score_values, '>>>>>>>>>Это результаты поражения')
            data_frame_score_values.to_csv('Scores.csv', encoding='utf8', index=False)

            df = data_frame_score_values.groupby(['UserID', 'Date']).sum()[['Choice']]

            print(df)

            df.to_csv('FinalScores.csv', encoding='utf8', index=True)

            self.final_score_public()

    def begin_betting(self, bet_check):
        # corrupted_message = str(bet_check.text)

        print(f'> Из БЕГИНИНГА> {bet_check}')

    def handle_message(self, received_msg):
        # while time.time() < self.timer:
        #     threading.Thread(target=self.thread_test_printer).start()

        if len(received_msg) == 0:
            return
        message = self.parse_message(received_msg)
        # print(f'> Из хэндла {received_msg}')

        # print(f'> Из хэндла {message}')

        if message.irc_command == 'PING':
            self.send_command('PONG :tmi.twitch.tv')

        if message.irc_command == 'PRIVMSG':
            # if message.text_command in TEMPLATE_COMMANDS:
            #     self.handle_template_command(
            #         message,
            #         message.text_command,
            #         TEMPLATE_COMMANDS[message.text_command],
            #     )

            if message.text_command in self.custom_commands:
                self.custom_commands[message.text_command](message)

            if message.text_command in self.bet_start and message.user in allowed_users:

                self.timer = (time.time() + seconds_for_timer * multiplier_for_timer)

                if not message.text_args:

                    text = f'Начинается голосование'
                    self.send_privmsg(message.channel, text)
                    print(f'Время пришло_______________ {self.timer}')

                else:

                    text = f'Начинается голосование {message.text_args}'
                    self.send_privmsg(message.channel, text)
                    print(f'Время пришло_______________ {self.timer}')

            elif message.text_command in self.bet_start and message.user != message.channel:

                text = f'Вам не доступна функция голосования'
                self.send_privmsg(message.channel, text)

            if message.text_command in self.bet_command:
                # self.write_a_csv(message)
                # print('Отправляем на обработку')
                # self.bet_check_lose(message)

                self.bet_check_win(message)
                print('Bet_Check_Win Running')

            if message.text_command in self.bet_stops and message.user == message.channel:

                print('Остановка голосования')
                self.betting_stop_check(message)

            else:
                pass
            # self.begin_betting(message)

    def loop_for_msgs(self):
        while True:
            received_msgs = self.irc.recv(2048).decode()
            for received_msg in received_msgs.split('\r\n'):
                self.handle_message(received_msg)


def main():
    bot = Bot()
    bot.connect()
    print('Hello')


if __name__ == '__main__':
    main()
    # threading.Thread(target=main).start()
