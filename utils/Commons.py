import logging
import pkgutil
import sys
from awsglue.utils import getResolvedOptions

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def get_job_arguments(arg_names):
    """
    :param arg_names: Names of the arguments to fetch
    :return: list of arguments after reading from environment
    """
    args = []
    if arg_names:
        args = getResolvedOptions(sys.argv, arg_names)
    logger.info("Returning job arguments : %s", args)
    return args


def get_optional_job_argument_value(arg_name):
    """
    :param arg_name: Names of the arguments to fetch
    :return: value of the optional argument if it is setup for the job, else None
    """
    arg_val = None
    logger.info("Checking for optional argument : %s in list : %s", arg_name, sys.argv)
    # appending -- to argument for checking, since the argument list has that appended but when using getResolvedOptions
    # we do not need that since it internally takes care of it
    if arg_name and ('--' + arg_name in sys.argv):
        arg_val = getResolvedOptions(sys.argv, [arg_name])[arg_name]
    logger.info("Value of optional argument : %s", arg_val)
    return arg_val


def get_local_file_content(root_folder, file_path):
    """
    :param root_folder: root folder under which to find the file
    :param file_path: path of file relative to root folder
    :return: content of the file as a string
    """
    return pkgutil.get_data(root_folder, file_path)


def get_distance_between_pair_of_coords(coord1, coord2):
    """
    "Inspired" from https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
    :param coord1: first location {lat, lng}
    :param coord2: second location {lat, lng}
    :return: distance in metre
    """
    from math import sin, cos, sqrt, atan2, radians

    # approximate radius of earth in m
    R = 6373.0

    print('coord data --------------', coord1, coord2)

    rad_lat_1 = radians(float(coord1['lat']))
    rad_lng_1 = radians(float(coord1['lng']))
    rad_lat_2 = radians(float(coord2['lat']))
    rad_lng_2 = radians(float(coord2['lng']))

    dlon = rad_lng_2 - rad_lng_1
    dlat = rad_lat_2 - rad_lat_1

    a = sin(dlat / 2) ** 2 + cos(rad_lat_1) * cos(rad_lat_2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance
