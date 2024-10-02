from datetime import datetime, date, timedelta

def today_date():
    date_ = str(date.today().strftime('%d-%m-%Y'))
    return date_

def time_now():
    time_now_ = str((datetime.now()).strftime('%d/%m/%Y %I:%M:%S %p'))
    return time_now_

