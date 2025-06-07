#!/usr/bin/python3

from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V4
from dsmr_parser.parsers import TelegramParser
from influxdb import InfluxDBClient
import config
import pprint
import decimal
import time
import os

while True:
    try:
        print("Connecting db")

        # Influx db settings
        db = InfluxDBClient(config.host,config.port, config.username, config.password, config.database)

        # Serial port settings and version
        serial_reader = SerialReader(
            device=config.serial_port,
            serial_settings=SERIAL_SETTINGS_V4,
            telegram_specification=telegram_specifications.BELGIUM_FLUVIUS
        )

        db.create_database('energy')

        # Read telegrams
        print("Waiting for P1 port measurement..")

        for telegram in serial_reader.read():
            influx_measurement={
                "measurement": "P1 values",
                "fields": {
                }
            }

            # Clear the screen for better UI experience
            os.system('clear')

            print(f"P1 message timestamp                     : {telegram.P1_MESSAGE_TIMESTAMP.value}")
            print(f"Electricity used (day)(total)       [kWh]: {telegram.ELECTRICITY_USED_TARIFF_1.value}")
            print(f"Electricity used (night)(total)     [kWh]: {telegram.ELECTRICITY_USED_TARIFF_2.value}")
            print(f"Electricity delivered (day)(total)  [kWh]: {telegram.ELECTRICITY_DELIVERED_TARIFF_1.value}")
            print(f"Electricity delivered (night)(total)[kWh]: {telegram.ELECTRICITY_DELIVERED_TARIFF_2.value}")
            print(f"Electricity active tariff (1-day/2-night): {telegram.ELECTRICITY_ACTIVE_TARIFF.value}")
            print(f"Electricity usage (now)              [kW]: {telegram.CURRENT_ELECTRICITY_USAGE.value}")
            print(f"Electricity delivery (now)           [kW]: {telegram.CURRENT_ELECTRICITY_DELIVERY.value}")
            print(f"Electricity average current (15 min) [kW]: {telegram.BELGIUM_CURRENT_AVERAGE_DEMAND.value}")
            print(f"Electricity maximum current month    [kW]: {telegram.BELGIUM_MAXIMUM_DEMAND_MONTH.value}")
            print(f"Power L1 positive (now)              [kW]: {telegram.INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE.value}")
            print(f"Power L1 negative (now)              [kW]: {telegram.INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE.value}")
            print(f"Voltage L1 (now)                      [V]: {telegram.INSTANTANEOUS_VOLTAGE_L1.value}")
            print(f"Current L1 (now)                      [A]: {telegram.INSTANTANEOUS_CURRENT_L1.value}")
            print(f"Gas used (total)                     [m3]: {telegram.get_mbus_device_by_channel(1).MBUS_METER_READING.value}")
            print(f"Water used (total)                   [m3]: {telegram.get_mbus_device_by_channel(2).MBUS_METER_READING.value}")

            influx_measurement['fields']['ELECTRICITY_USED_TARIFF_1']              = float(telegram.ELECTRICITY_USED_TARIFF_1.value)
            influx_measurement['fields']['ELECTRICITY_USED_TARIFF_2']              = float(telegram.ELECTRICITY_USED_TARIFF_2.value)
            influx_measurement['fields']['ELECTRICITY_DELIVERED_TARIFF_1']         = float(telegram.ELECTRICITY_DELIVERED_TARIFF_1.value)
            influx_measurement['fields']['ELECTRICITY_DELIVERED_TARIFF_2']         = float(telegram.ELECTRICITY_DELIVERED_TARIFF_2.value)
            influx_measurement['fields']['ELECTRICITY_ACTIVE_TARIFF']              = int(telegram.ELECTRICITY_ACTIVE_TARIFF.value)
            influx_measurement['fields']['CURRENT_ELECTRICITY_USAGE']              = float(telegram.CURRENT_ELECTRICITY_USAGE.value)
            influx_measurement['fields']['CURRENT_ELECTRICITY_DELIVERY']           = float(telegram.CURRENT_ELECTRICITY_DELIVERY.value)
            influx_measurement['fields']['BELGIUM_CURRENT_AVERAGE_DEMAND']         = float(telegram.BELGIUM_CURRENT_AVERAGE_DEMAND.value)
            influx_measurement['fields']['BELGIUM_MAXIMUM_DEMAND_MONTH']           = float(telegram.BELGIUM_MAXIMUM_DEMAND_MONTH.value)
            influx_measurement['fields']['INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE'] = float(telegram.INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE.value)
            influx_measurement['fields']['INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE'] = float(telegram.INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE.value)
            influx_measurement['fields']['INSTANTANEOUS_VOLTAGE_L1']               = float(telegram.INSTANTANEOUS_VOLTAGE_L1.value)
            influx_measurement['fields']['INSTANTANEOUS_CURRENT_L1']               = float(telegram.INSTANTANEOUS_CURRENT_L1.value)
            influx_measurement['fields']['MBUS_METER_READING_CHANNEL1']            = float(telegram.get_mbus_device_by_channel(1).MBUS_METER_READING.value)
            influx_measurement['fields']['MBUS_METER_READING_CHANNEL2']            = float(telegram.get_mbus_device_by_channel(2).MBUS_METER_READING.value)

            #pprint.pprint(influx_measurement)
            if len(influx_measurement['fields']):
                db.write_points([influx_measurement])
    except Exception as e:
        print(str(e))
        print("Pausing and restarting...")
        time.sleep(10)

