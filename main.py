#!/usr/bin/env python3

import configparser
import mmap
from operator import truediv
import sys
from datetime import datetime

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import Point, WritePrecision

from datalog import DataLogReader

config_filename = "config.ini"
BATCH_SIZE = 1000

def read_config(filename):
    config = configparser.ConfigParser()
    try:
        config.read(filename)
        return dict(config["datalog-influx"])
    except configparser.ParsingError as exc:
        print("problem parsing config file:", str(exc), file=sys.stderr)

def main():
    config = read_config(config_filename)
    bucket = config["bucket"]

    client = influxdb_client.InfluxDBClient.from_config_file(config_filename)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    warned_types = set()

    if len(sys.argv) != 2:
        print("Usage: main.py <file>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1], "r") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        reader = DataLogReader(mm)
        if not reader:
            print("not a log file", file=sys.stderr)
            sys.exit(1)

        entries = {}
        baseTime = None
        prevTime = None
        p = None
        batch = []

        for record in reader:
            timestamp = record.timestamp / 1000
            if record.isStart():
                try:
                    data = record.getStartData()
                    print(
                        f"Start({data.entry}, name='{data.name}', type='{data.type}', metadata='{data.metadata}') [{timestamp}]"
                    )
                    if data.entry in entries:
                        print("...DUPLICATE entry ID, overriding")
                    entries[data.entry] = data
                except TypeError as e:
                    print("Start(INVALID)")
            elif record.isFinish():
                try:
                    entry = record.getFinishEntry()
                    print(f"Finish({entry}) [{timestamp}]")
                    if entry not in entries:
                        print("...ID not found")
                    else:
                        del entries[entry]
                except TypeError as e:
                    print("Finish(INVALID)")
            elif record.isSetMetadata():
                try:
                    data = record.getSetMetadataData()
                    print(f"SetMetadata({data.entry}, '{data.metadata}') [{timestamp}]")
                    if data.entry not in entries:
                        print("...ID not found")
                except TypeError as e:
                    print("SetMetadata(INVALID)")
            elif record.isControl():
                print("Unrecognized control record")
            else:
                #print(f"Data({record.entry}, size={len(record.data)}) ", end="")
                entry = entries.get(record.entry, None)
                if entry is None:
                    print("<ID not found>")
                    continue
                #print(f"<name='{entry.name}', type='{entry.type}'> [{timestamp}]")

                try:
                    # handle systemTime specially
                    if entry.name == "systemTime" and entry.type == "int64":
                        dt = datetime.fromtimestamp(record.getInteger() / 1000000)
                        baseTime = record.getInteger() / 1000 - timestamp
                        #print("  {:%Y-%m-%d %H:%M:%S.%f}".format(dt))
                        continue

                    value = None
                    
                    if entry.type == "double":
                        value = record.getDouble()
                        #print(f"  {record.getDouble()}")
                    elif entry.type == "int64":
                        value = record.getInteger()
                        #print(f"  {record.getInteger()}")
                    elif entry.type == "string":
                        value = record.getString()
                        #print(f"  '{record.getString()}'")
                    elif entry.type == "json":
                        #print(f"  '{record.getString()}'")
                        pass
                    elif entry.type == "boolean":
                        value = record.getBoolean()
                        #print(f"  {record.getBoolean()}")
                    elif entry.type == "boolean[]":
                        value = record.getBooleanArray()
                        #print(f"  {arr}")
                    elif entry.type == "double[]":
                        value = record.getDoubleArray().tolist()
                        #print(f"  {arr}")
                    elif entry.type == "float[]":
                        value = record.getFloatArray().tolist()
                        #print(f"  {arr}")
                    elif entry.type == "int64[]":
                        value = record.getIntegerArray().tolist()
                        #print(f"  {arr}")
                    elif entry.type == "string[]":
                        value = record.getStringArray()
                        #print(f"  {arr}")

                    if value is None:
                        if entry.type not in warned_types:
                            print("skipping unsupported type: " + entry.type)
                            warned_types.add(entry.type)
                    elif baseTime is not None:
                        t = int(baseTime + timestamp)

                        if isinstance(value, list):
                            listMode = True
                        else:
                            listMode = False
                            value = [value]
                        
                        for index, v in enumerate(value):
                            if p is None:
                                p = Point("robot").time(t, write_precision=WritePrecision.MS)

                            if listMode:
                                name = f"{entry.name}/{index}"
                            else:
                                name = entry.name
                            
                            p.field(name, v)
                            # print(datetime.fromtimestamp(t/1000).isoformat(" "))

                            if t != prevTime:
                                batch.append(p)
                                prevTime = t
                                p = None

                            if len(batch) == BATCH_SIZE:
                                write_api.write(bucket=bucket, record=batch)
                                batch = []

                except TypeError as e:
                    print("  invalid")
        if p:
            batch.append(p)
            p = None
        
        if batch:
            write_api.write(bucket=bucket, record=batch)
            batch = []

main()
