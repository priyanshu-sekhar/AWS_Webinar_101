from datetime import datetime, timedelta
import time
from utils import Constants

from Constants import YEAR_TO_DATE_DASH_FORMAT


def fetch_past_date(offset_from_date, reference_date=None):
    """
    :param offset_from_date: number of days to offset in the past from current date or job_date
    :param reference_date: date from which past date needs to be fetched
    :return: Date generated after subtracting the offset
    """
    # datetime.now() - timedelta(days=offset) gives <offset> dates prev to curr date.
    if not reference_date:
        return datetime.now() - timedelta(days=offset_from_date)
    else:
        return reference_date - timedelta(days=offset_from_date)


def format_date(date, str_format=YEAR_TO_DATE_DASH_FORMAT):
    """
    Formats the passed date in the desired format
    :param date: Date to be converted
    :param str_format: format to convert the date to
    :return: String representing the date in the given format
    """
    return date.strftime(str_format)


def fetch_past_date_string(offset_from_curr_date, str_format):
    """
    Returns the past date in the string format passed
    :param offset_from_curr_date:number of days to offset in the past from current date
    :param str_format: format to convert the date to
    :return: formatted date
    """
    return format_date(date=fetch_past_date(offset_from_date=offset_from_curr_date), str_format=str_format)


def get_epoch_in_millis_for_day(offset_from_curr_day):
    """
    Gets epoch time for curr_date - offset_from_curr_day
    :param offset_from_curr_day: number of days to offset in the past from current date
    :return: epoch days corresponding to the passed date
    """

    date_format = YEAR_TO_DATE_DASH_FORMAT
    date_str = fetch_past_date_string(
        offset_from_curr_date=offset_from_curr_day,
        str_format=date_format
    )
    return int(time.mktime(time.strptime(date_str, date_format))) * 1000
