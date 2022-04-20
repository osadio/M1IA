import argparse
import sys
from time import sleep

def parse_command_line_args():
  """Parse command line arguments."""
  parser = argparse.ArgumentParser(description=(
    'Example Google Cloud IoT Core MQTT device connection code.'))
  parser.add_argument(
    '--serial_port',
    default='/dev/ttyACM0',
    help='Serial port device connected to the Arduino.')
  return parser.parse_args()


def read_sensors(ser):
  """Read Arduino sensors from serial interface"""
  #humidity, temperature = 0.0, 0.0
  try:
      response = serial_receive(ser)
      response = response.rstrip().decode()
      print('Received from Arduino: {}'.format(response))
      if response[0] == '#':
          water_level = response.split('#')[1]
      else:
          print('Error getting Arduino sensor values over serial')
          return None, None
  except IOError:
    print('I/O Error')
    return None
  return water_level

def serial_receive(ser):
  """Write string to serial connection and return any response."""
  while True:
    try:
      sleep(0.01)
      state = ser.readline()
      if state:
        return state
    except:
      pass
  sleep(0.1)
  return 'E'

def init_serial(serial_port):
  import serial
  print('Creating and flushing serial port.')
  ser = serial.Serial(serial_port)
  with ser:
    ser.setDTR(False)
    sleep(1)
    ser.flushInput()
    ser.setDTR(True)
  ser = serial.Serial(serial_port, 9600, timeout=0.1)
  return ser

def main(argv):
  args = parse_command_line_args()
  ser = init_serial(args.serial_port)
  while True:
    water_level = read_sensors(ser)
    print(water_level)
    sleep(1)

if __name__ == '__main__':
  main(sys.argv)
