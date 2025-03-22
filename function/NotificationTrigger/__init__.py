import logging
import azure.functions as func
import psycopg2
import psycopg2.extras
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(os.environ["dbname"], os.environ["dbuser"], os.environ["host"], os.environ["password"]))
    conn.autocommit = True
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    logging.info("Connected to database")

    try:
        # TODO: Get notification message and subject from database using the notification_id
        curs.execute("SELECT * FROM public.notification WHERE id = {}".format(notification_id))
        row = curs.fetchone()
        message = row['message']
        subject = row['subject']
        
        # TODO: Get attendees email and name
        curs.execute("SELECT first_name, last_name, email from attendee")
        attendees = curs.fetchall()

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            emailsubject = '{}: {}'.format(attendee['first_name'], subject)
            send_email(attendee['email'], emailsubject, message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        updateSql = "UPDATE notification SET completed_date = '{}', status = 'Notified {} attendees' WHERE id = {}".format(datetime.now(), len(attendees), notification_id)
        logging.info(updateSql)
        curs.execute(updateSql)
        logging.info("Record updated")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        conn.close()
        logging.info("Connection closed")

def send_email(email, subject, body):
    logging.info("Sending email for {} with subject {}".format(email, subject))
    if not os.environ["SENDGRID_API_KEY"]:
        message = Mail(
            from_email=os.environ['ADMIN_EMAIL_ADDRESS'],
            to_emails=email,
            subject=subject,
            plain_text_content=body)

        sg = SendGridAPIClient(os.environ['SENDGRID_API_KEY'])
        sg.send(message)
