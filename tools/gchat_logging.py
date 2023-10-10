from json import dumps
from config import DAILY_GENIE_EVALUATION_G_CHAT_KEY, HEADER
from httplib2 import Http
# import smtplib


def send_to_g_chat(data):
    url = DAILY_GENIE_EVALUATION_G_CHAT_KEY

    bot_message = {
        'text': str(data)}
    bot_message = dumps(bot_message)
    message_headers = {'Content-Type': f'{HEADER}; charset=UTF-8'}
    http_obj = Http()
    http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=bot_message,
    )
    return "Message has sent to g_chat"


if __name__ == '__main__':
    res = send_to_g_chat(data="Genie Evaluation group initialized")
    print(res)
