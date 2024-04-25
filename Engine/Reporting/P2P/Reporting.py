import MetricsFunctions
import smtplib, ssl
from pyspark.sql import SparkSession

class Reporting:

    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName("PathToPurchaseReport").getOrCreate()
        self.metrics_functions = MetricsFunctions(self.spark)
        self.metrics_methods = dir(self.metrics_functions)
        self.all_methods_except_init = [attr for attr in self.metrics_methods if callable(getattr(MetricsFunctions, attr)) and attr != "__init__"]
        self.report = ""

    def create_report(self) -> None:

        self.report += f"""
            <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
            <html lang="en" xmlns="http://www.w3.org/1999/xhtml">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
                <meta http-equiv="X-UA-Compatible" content="IE=edge">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Pattern Feed Daily Monitoring</title>
            </head>
            <body>
                <p class="title" style="font-size: 15px; margin-bottom: -10px;">[OpenBean – Clickstream monitoring]: Path-to-purchase Daily Monitoring – counting & time series metrics</p>"""

        for metric in self.all_methods_except_init:
            method_to_call = getattr(self.metrics_functions, metric)
            insert_position = self.report.find("</div>")
            result = method_to_call()
            self.report += self.report[:insert_position] + result + self.report[insert_position:]


    def send_report(self) -> None:
        smtp_server = "smtp.open-bean.com"
        port = 587
        sender_email = "tomas.korec@open-bean.com"
        password = "password"
        context = ssl.create_default_context()
        try:
            server = smtplib.SMTP(smtp_server, port)
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, self.report)
        finally:
            server.quit()
