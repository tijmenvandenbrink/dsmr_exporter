#!/usr/bin/env python3
import os
import random
import time
import logging
import argparse

import pdb

from prometheus_client import start_http_server, Counter, Gauge, Histogram, Info

from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
from dsmr_parser.objects import CosemObject, MBusObject, Telegram
from dsmr_parser.parsers import TelegramParser

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

i = Info('dsmr_exporter', 'DSMR exporter for the Dutch Digitale Slimme Meter')
i.info({'version': '0.0.1'})

serial_reader = SerialReader(
    device='/dev/ttyUSB0',
    serial_settings=SERIAL_SETTINGS_V5,
    telegram_specification=telegram_specifications.V5
)

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,
    handlers=[logging.FileHandler("dsmr_exporter.log"),
              logging.StreamHandler()],
    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("""dsmr_exporter.py - Expose readings from the DSMR reader in Prometheus format

Press Ctrl+C to exit!

""")

"""
Example telegram

P1_MESSAGE_HEADER: 	 50	[None]
P1_MESSAGE_TIMESTAMP: 	 2020-12-25T09:09:36+01:00	[None]
EQUIPMENT_IDENTIFIER: 	 4530303437303030303731393632383139	[None]
ELECTRICITY_USED_TARIFF_1: 	 122.976	[kWh]
ELECTRICITY_USED_TARIFF_2: 	 81.744	[kWh]
ELECTRICITY_DELIVERED_TARIFF_1: 	 0.004	[kWh]
ELECTRICITY_DELIVERED_TARIFF_2: 	 0.000	[kWh]
ELECTRICITY_ACTIVE_TARIFF: 	 0001	[None]
CURRENT_ELECTRICITY_USAGE: 	 3.731	[kW]
CURRENT_ELECTRICITY_DELIVERY: 	 0.000	[kW]
LONG_POWER_FAILURE_COUNT: 	 2	[None]
SHORT_POWER_FAILURE_COUNT: 	 3	[None]
POWER_EVENT_FAILURE_LOG: 	 	 buffer length: 1
	 buffer type: 0-0:96.7.19
	 event occured at: 2019-10-08T20:27:22+02:00	 for: 18372 [s]
VOLTAGE_SAG_L1_COUNT: 	 2	[None]
VOLTAGE_SWELL_L1_COUNT: 	 0	[None]
INSTANTANEOUS_VOLTAGE_L1: 	 232.0	[V]
INSTANTANEOUS_CURRENT_L1: 	 16	[A]
TEXT_MESSAGE: 	 None	[None]
DEVICE_TYPE: 	 3	[None]
INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE: 	 3.731	[kW]
INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE: 	 0.000	[kW]
EQUIPMENT_IDENTIFIER_GAS: 	 4730303732303034303036333536333230	[None]
HOURLY_GAS_METER_READING: 	 134.906	[m3] at 2020-12-25T09:05:00+01:00
"""

DSMR_LOCATION = os.getenv('DSMR_LOCATION', 'Adelaide')
DSMR_READ_INTERVAL = os.getenv('DSMR_READ_INTERVAL', 60)
DEBUG = os.getenv('DEBUG', 'false')

# Setup InfluxDB
INFLUXDB_URL = os.getenv('INFLUXDB_URL', '')
INFLUXDB_USERNAME = os.getenv('INFLUXDB_USERNAME', '')
INFLUXDB_PASSWORD = os.getenv('INFLUXDB_PASSWORD', '')
INFLUXDB_ORG_ID = os.getenv('INFLUXDB_ORG_ID', '')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'dsmr')
INFLUXDB_TIME_BETWEEN_POSTS = int(os.getenv('INFLUXDB_TIME_BETWEEN_POSTS', '5'))
INFLUXDB_VERIFY_SSL = os.getenv('INFLUXDB_VERIFY_SSL')

influxdb_client = InfluxDBClient(url=INFLUXDB_URL, token=f'{INFLUXDB_USERNAME}:{INFLUXDB_PASSWORD}', org=INFLUXDB_ORG_ID, verify_ssl=False)
influxdb_api = influxdb_client.write_api(write_options=SYNCHRONOUS)


# Interesting Telegram
P1_MESSAGE_HEADER = Gauge('p1_message_header','Message header count')
ELECTRICITY_USED_TARIFF_1 = Gauge('electricity_used_tariff_1','Electricity used tariff 1 (kWh)')
ELECTRICITY_USED_TARIFF_2 = Gauge('electricity_used_tariff_2','Electricity used tariff 2 (kWh)')
ELECTRICITY_DELIVERED_TARIFF_1 = Gauge('electricity_delivered_tariff_1','Electricity delivered tariff 1 (kWh)')
ELECTRICITY_DELIVERED_TARIFF_2 = Gauge('electricity_delivered_tariff_2','Electricity delivered tariff 2 (kWh)')
ELECTRICITY_ACTIVE_TARIFF = Gauge('electricity_active_tariff','Electricity active tariff')
CURRENT_ELECTRICITY_USAGE = Gauge('current_electricity_usage','Current electricity usage (kW)')
CURRENT_ELECTRICITY_DELIVERY = Gauge('current_electricity_delivery','Current electricity delivery (kW)')
LONG_POWER_FAILURE_COUNT = Gauge('long_power_failure_count','Long power failure count')
SHORT_POWER_FAILURE_COUNT = Gauge('short_power_failure_count','Short power failure count')
VOLTAGE_SAG_L1_COUNT = Gauge('voltage_sag_l1_count','Voltage sag L1 count')
VOLTAGE_SWELL_L1_COUNT = Gauge('voltage_swell_l1_count','Voltage swell L1 count')
INSTANTANEOUS_VOLTAGE_L1 = Gauge('instantaneous_voltage_l1','Instantaneous voltage L1 (V)')
INSTANTANEOUS_CURRENT_L1 = Gauge('instantaneous_current_l1','Instantaneous current L1 (A)')
INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE = Gauge('instantaneous_active_power_l1_positive','Instantaneous active power l1 positive (kW)')
INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE = Gauge('instantaneous_active_power_l1_negative','Instantaneous active power l1 negative (kW)')
HOURLY_GAS_METER_READING = Gauge('hourly_gas_meter_reading','Hourly gas meter reading (m3)')


def get_dsmr_readings():
    """Get the DSMR readings"""
    telegram = next(serial_reader.read_as_object())

    return telegram

def post_to_prometheus(telegram):
    #for telegram in serial_reader.read_as_object():
    #    os.system('clear')
    #    print(telegram)
    ELECTRICITY_USED_TARIFF_1.set(telegram.ELECTRICITY_USED_TARIFF_1.value)
    ELECTRICITY_USED_TARIFF_2.set(telegram.ELECTRICITY_USED_TARIFF_2.value)
    ELECTRICITY_DELIVERED_TARIFF_1.set(telegram.ELECTRICITY_DELIVERED_TARIFF_1.value)
    ELECTRICITY_DELIVERED_TARIFF_2.set(telegram.ELECTRICITY_DELIVERED_TARIFF_2.value)
    ELECTRICITY_ACTIVE_TARIFF.set(telegram.ELECTRICITY_ACTIVE_TARIFF.value)
    CURRENT_ELECTRICITY_USAGE.set(telegram.CURRENT_ELECTRICITY_USAGE.value)
    CURRENT_ELECTRICITY_DELIVERY.set(telegram.CURRENT_ELECTRICITY_DELIVERY.value)
    LONG_POWER_FAILURE_COUNT.set(telegram.LONG_POWER_FAILURE_COUNT.value)
    SHORT_POWER_FAILURE_COUNT.set(telegram.SHORT_POWER_FAILURE_COUNT.value)
    VOLTAGE_SAG_L1_COUNT.set(telegram.VOLTAGE_SAG_L1_COUNT.value)
    VOLTAGE_SWELL_L1_COUNT.set(telegram.VOLTAGE_SWELL_L1_COUNT.value)
    INSTANTANEOUS_VOLTAGE_L1.set(telegram.INSTANTANEOUS_VOLTAGE_L1.value)
    INSTANTANEOUS_CURRENT_L1.set(telegram.INSTANTANEOUS_CURRENT_L1.value)
    INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE.set(telegram.INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE.value)
    INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE.set(telegram.INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE.value)
    HOURLY_GAS_METER_READING.set(telegram.HOURLY_GAS_METER_READING.value)

def post_to_influxdb(telegram):
    """Post all dsmr data to InfluxDB"""
    fields = ("ELECTRICITY_USED_TARIFF_1",
              "ELECTRICITY_USED_TARIFF_2",
              "ELECTRICITY_DELIVERED_TARIFF_1",
              "ELECTRICITY_DELIVERED_TARIFF_2",
              "ELECTRICITY_ACTIVE_TARIFF",
              "CURRENT_ELECTRICITY_USAGE",
              "CURRENT_ELECTRICITY_DELIVERY",
              "LONG_POWER_FAILURE_COUNT",
              "VOLTAGE_SAG_L1_COUNT",
              "VOLTAGE_SAG_L2_COUNT",
              "VOLTAGE_SAG_L3_COUNT",
              "VOLTAGE_SWELL_L1_COUNT",
              "VOLTAGE_SWELL_L2_COUNT",
              "VOLTAGE_SWELL_L3_COUNT",
              "INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE",
              "INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE",
              "INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE",
              "INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE",
              "INSTANTANEOUS_ACTIVE_POWER_L2_NEGATIVE",
              "INSTANTANEOUS_ACTIVE_POWER_L3_NEGATIVE",
              "HOURLY_GAS_METER_READING")

    name = 'dsmr'
    tag = ['location', DSMR_LOCATION]
    epoch_time_now = round(time.time())
    data_points = []
    for field_name, value in telegram:
        if field_name in fields:
            try:
                norm_value = float(value.value)
            except SyntaxError:
                norm_value = int(value.value)

            data_points.append(Point('dsmr').tag('location', DSMR_LOCATION).field(field_name, norm_value))
            if DEBUG:
                logging.info("Added datapoint: Field {}, Value: {}, Normalized Value: {}".format(field_name, value, norm_value))
    try:
        influxdb_api.write(bucket=INFLUXDB_BUCKET, record=data_points)
        if DEBUG:
            logging.info('InfluxDB response: OK')
    except Exception as exception:
        logging.warning('Exception sending to InfluxDB: {}'.format(exception))

def str_to_bool(value):
    if value.lower() in {'false', 'f', '0', 'no', 'n'}:
        return False
    elif value.lower() in {'true', 't', '1', 'yes', 'y'}:
        return True
    raise ValueError('{} is not a valid boolean value'.format(value))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--bind", metavar='ADDRESS', default='0.0.0.0', help="Specify alternate bind address [default: 0.0.0.0]")
    parser.add_argument("-p", "--port", metavar='PORT', default=8000, type=int, help="Specify alternate port [default: 8000]")
    parser.add_argument("-d", "--debug", metavar='DEBUG', type=str_to_bool, help="Turns on more verbose logging, showing sensor output and post responses [default: false]")
    parser.add_argument("-i", "--influxdb", metavar='INFLUXDB', type=str_to_bool, default='false', help="Post sensor data to InfluxDB [default: false]")
    args = parser.parse_args()

    # Start up the server to expose the metrics.
    start_http_server(addr=args.bind, port=args.port)
    # Generate some requests.

    if args.debug:
        DEBUG = True

    logging.info("Listening on http://{}:{}".format(args.bind, args.port))

    while True:
        try:
            telegram = get_dsmr_readings()
            post_to_prometheus(telegram)
            if args.influxdb:
                post_to_influxdb(telegram)
        except Exception as exception:
            logging.warning('Exception getting dsmr readings: {}'.format(exception))
            pass

        time.sleep(int(DSMR_READ_INTERVAL))
        if DEBUG:
            logging.debug('Sensor data: {}'.format(get_dsmr_readings()))
