# -*- coding: utf-8 -*-
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import sys

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("send mail fail,argument less 3\n");
        sys.exit(1)
    send_from = 'jumpserver@idongjia.cn'
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['Date'] = formatdate(localtime=True)
    msg['to'] = sys.argv[1] 
    msg['Subject'] = sys.argv[2] 
    send_all = sys.argv[1].split(",")

    msg.attach(MIMEText(sys.argv[2],'plain','utf-8'))

    smtp = smtplib.SMTP_SSL('smtp.exmail.qq.com')
    #smtp.connect('smtp.gooagoo.com')
    smtp.login(send_from,'aS7WSGwEJBtwh4re')
    smtp.sendmail(send_from, send_all, msg.as_string())
    smtp.close()
