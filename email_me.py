import smtplib
import time
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart


def send_email(files_list: list[str], subject: str):

    print('\nStart')
    sender = 's-3m@yandex.ru'
    password = 'aplxoqrzhjashimq'

    server = smtplib.SMTP('smtp.yandex.ru', 587)
    server.starttls()

    print('Login...')
    server.login('s-3m', password)
    msg = MIMEMultipart()
    print('Login success')
    msg["From"] = sender
    msg["To"] = sender
    msg["Subject"] = subject

    for file in files_list:
        filename = file.split("/")[-1]
        with open(file, 'rb') as f:
            file = MIMEApplication(f.read(), 'xlsx')
            file.add_header('content-disposition', 'attachment', filename=filename)
            msg.attach(file)

    time.sleep(30)
    print('sending...')
    server.sendmail(sender, sender, msg.as_string())
    print('Success')
    server.close()
