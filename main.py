import asyncio, sys, os, time, math, datetime
# required by aiomqtt: https://pypi.org/project/aiomqtt/
if sys.platform.lower() == "win32" or os.name.lower() == "nt":
	from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
	set_event_loop_policy(WindowsSelectorEventLoopPolicy())

import pymodbus.client as ModbusClient
from pymodbus import (
    ExceptionResponse,
    Framer,
    ModbusException,
    pymodbus_apply_logging_config,
)

import sqlite3
import struct


class DBLogger(object):
	"""sqlite3 database per device"""

	def __init__(self, addr="1"):

		self.conn = sqlite3.connect(f"decibels-{addr}.db")

		self.db = self.conn.cursor()
		self.db.execute("""CREATE TABLE if not exists timeseries (
								id integer primary key,
								time integer default (cast(strftime('%s','now') as int)),
								decidecibels integer
								) STRICT;""")
		self.conn.commit()

	def store_measurement(self, decibels: float):
		self.db.execute(
		    """INSERT INTO timeseries
								(decidecibels) 
								VALUES (?);""", (int(decibels * 10),))
		self.conn.commit()


def main():
	framer = Framer.RTU
	port = "COM12"
	#pymodbus_apply_logging_config("DEBUG")

	client = ModbusClient.ModbusSerialClient(
	    port,
	    framer=framer,
	    timeout=1,
	    retries=2,
	    retry_on_empty=True,
	    # strict=True,
	    baudrate=4800,
	    bytesize=8,
	    parity="N",
	    stopbits=1,
	    # handle_local_echo=False,
	)

	print("connect to server")
	client.connect()
	# test client is connected
	assert client.connected

	print("get and verify data")
	dblogger = DBLogger(datetime.date.today().strftime("%Y-%m-%d"))
	while True:
		try:
			rr = client.read_input_registers(0, 1, slave=1)
		except ModbusException as exc:
			print(f"Received ModbusException({exc}) from library")
			client.close()
			return
		if rr.isError():
			print(f"Received Modbus library error({rr})")
			client.close()
			return
		if isinstance(rr, ExceptionResponse):
			print(f"Received Modbus library exception ({rr})")
			client.close()
		db = rr.registers[0] / 10
		dblogger.store_measurement(db)
		time.sleep(0.5)
		print(db)

	print("close connection")
	client.close()


if __name__ == "__main__":
	main()
