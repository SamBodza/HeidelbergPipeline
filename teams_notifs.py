import pymsteams
from config_parser import get_config

colours = {'red': 'E74C3C',
           'green': '58D68D'}


def send_teams_notif(url, Title, msg, colour):
    # You must create the connectorcard object with the Microsoft Webhook URL
    myTeamsMessage = pymsteams.connectorcard(url)

    # add Title to Card
    myTeamsMessage.title(Title)

    # Add text to the message.
    myTeamsMessage.text(msg)

    # add colour to card
    myTeamsMessage.color(colour)

    # send the message.
    myTeamsMessage.send()


def send_error_msg(msg):
    send_teams_notif(get_config()['TEAMS']['webhook'],
                     'ERROR',
                     msg,
                     colours['red'])


def send_succ_msg(msg):
    send_teams_notif(get_config()['TEAMS']['webhook'],
                     'SUCCESS',
                     msg,
                     colours['green'])


if __name__ == '__main__':
    send_error_msg('this is a test')
