# Pushy push server
# Written by Somsubhra

# All imports
import socket
import select
import sqlite3
import hashlib
import signal
import sys


class Pushy:


  # Constructor for the Pushy server
  def __init__(self, host, port, recv_buffer_size, db_name):

    self.host = host
    self.port = port
    self.recv_buffer_size = recv_buffer_size
    self.db_name = db_name

    # Signal handers
    signal.signal(signal.SIGINT, self.shut_down)

    # Setup database
    try:
      db = sqlite3.connect(self.db_name)
      db_cursor = db.cursor()

      # Set up the channels table
      query = '''
        CREATE TABLE IF NOT EXISTS channel (
          id INTEGER NOT NULL,
          name TEXT NOT NULL,
          password TEXT NOT NULL,
          creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (id)
        )
      '''
      db_cursor.execute(query)
      db.commit()

      # Set up the subscribers table
      query = '''
        CREATE TABLE IF NOT EXISTS subscriber (
          publisher_id INTEGER NOT NULL,
          subscriber_id INTEGER NOT NULL,
          subscription_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (publisher_id, subscriber_id),
          FOREIGN KEY (publisher_id) REFERENCES channel(id),
          FOREIGN KEY (subscriber_id) REFERENCES channel(id)
        )
      '''
      db_cursor.execute(query)
      db.commit()

    except Exception as e:
      db.rollback()
      print "Error: " + str(e)

    finally:
      db.close()

    # Set up Pushy server's socket
    self.connection_list = []
    self.identified_connections = {}

    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server_socket.bind((self.host, self.port))
    self.server_socket.listen(10)

    self.connection_list.append(self.server_socket)


  # The main loop of Pushy server
  def run(self):
    print "-- Pushy listening on port " + str(self.port)

    while True:

      read_sockets, write_sockets, error_sockets = select.select(self.connection_list, [], [])

      for sock in read_sockets:

        if sock == self.server_socket:

          # Accept incoming connections to server
          sockfd, addr = self.server_socket.accept()
          self.connection_list.append(sockfd)
          print "-- Client (%s %s) connected to Pushy" % addr

        else:

          # Process data from clients
          try:
            data = sock.recv(self.recv_buffer_size)
            if data:
              print "-- New message from" + str(sock.getpeername()) + ": " + data

              if self.is_command(data):
                self.exec_command(data, sock)

          except:
            print "-- Client (%s, %s) disconnected from Pushy" % addr
            sock.close()
            self.connection_list.remove(sock)
            self.identified_connections.pop(sock)
            continue

    self.server_socket.close()


  # Return whether the message is a command or not
  def is_command(self, message):
    return message[0] == '/'

  # Execute the command
  def exec_command(self, message, socket):
    message = message.split()

    command = message[0]
    command = command[1:]

    args = []

    if len(message) > 1:
      args = message[1:]

    print "-- Executing " + command + " command with args " + str(args)

    commands = {
      'reg': self.register,
      'id': self.identify,
      'pub': self.publish,
      'sub': self.subscribe
    }

    if command in commands:
      commands[command](args, socket)
    else:
      print "Error: Invalid command"


  # Register command
  def register(self, args, socket):

    if len(args) < 3:
      print "Usage: /reg <id> <name> <pass>"
      return

    print "-- Registering " + str(args)

    try:
      db = sqlite3.connect(self.db_name)
      db_cursor = db.cursor()

      query = '''
        INSERT INTO channel(id, name, password)
        VALUES(?, ?, ?)
      '''

      sha = hashlib.sha1()
      sha.update(args[2])

      db_cursor.execute(query, (args[0], args[1], sha.hexdigest()))
      db.commit()
      print "-- Registered channel " + args[0]

    except Exception as e:
      db.rollback()
      print "Error: " + str(e)

    finally:
      db.close()


  # Identify command
  def identify(self, args, socket):

    if len(args) < 2:
      print "Usage: /id <channel_id> <password>"
      return

    print "-- Identifying " + str(args)

    try:
      db = sqlite3.connect(self.db_name)
      db_cursor = db.cursor()

      query = '''
        SELECT id FROM channel
        WHERE id=? AND password=?
      '''

      sha = hashlib.sha1()
      sha.update(args[1])

      db_cursor.execute(query, (args[0], sha.hexdigest()))
      channel = db_cursor.fetchall()

      if len(channel) != 1:
        print "Error: Identification failure"
        return

      for row in channel:
        channel_id = row[0]
        self.identified_connections[socket] = str(channel_id)
        print "-- Channel " + str(channel_id) + " identified"

    except Exception as e:
      db.rollback()
      print "Error: " + str(e)

    finally:
      db.close()


  # Publish to channel
  def publish(self, args, socket):

    if not self.is_identified_connection(socket):
      print "Error: Need to be identified first"
      return

    if len(args) < 1:
      print "Usage: /pub <your_message>"
      return

    publisher_id = self.identified_connections[socket]
    message = ' '.join(args)

    print "-- Publishing " + str(args)

    try:
      db = sqlite3.connect(self.db_name)
      db_cursor = db.cursor()

      query = '''
        SELECT subscriber_id FROM subscriber
        WHERE publisher_id=?
      '''

      db_cursor.execute(query, (publisher_id,))
      subscriber_ids = db_cursor.fetchall()

      subscribers = []

      for subscriber_id in subscriber_ids:
        subscribers.append(str(subscriber_id[0]))

      self.message_clients(message, subscribers, publisher_id)

      print "-- Published message to subscribers"

    except Exception as e:
      db.rollback()
      print "Error: " + str(e)

    finally:
      db.close()


  # Subscribe to channel
  def subscribe(self, args, socket):

    if not self.is_identified_connection(socket):
      print "Error: Need to be identified first"
      return

    if len(args) < 1:
      print "Usage: /sub <publisher_id>"
      return

    print "-- Subscribing " + str(args)

    try:
      db = sqlite3.connect(self.db_name)
      db_cursor = db.cursor()

      query = '''
        INSERT INTO subscriber(publisher_id, subscriber_id)
        VALUES(?, ?)
      '''

      db_cursor.execute(query, (args[0], self.identified_connections[socket]))
      db.commit()
      print "-- Subscribed to channel " + args[0]

    except Exception as e:
      db.rollback()
      print "Error: " + str(e)

    finally:
      db.close()


  # Send message to clients
  def message_clients(self, message, subscriber_ids, publisher_id):

    message = str(publisher_id) + ": " + message

    print message
    print subscriber_ids

    for socket in self.identified_connections:

      if self.identified_connections[socket] in subscriber_ids:

        try:
          socket.send(message)

        except:
          socket.close()
          self.connection_list.remove(socket)
          self.identified_connections.pop(socket)


  # Check if connection is identified
  def is_identified_connection(self, socket):
    return socket in self.identified_connections


  # Shut down the push server
  def shut_down(self, signal, frame):
    print "\n-- Ctrl+C caught"
    print "-- Pushy server exiting gracefully"
    sys.exit(0)


# The main method of the program
def main():
  pushy_server = Pushy("0.0.0.0", 5000, 4096, 'pushy_data.db')
  pushy_server.run()


if __name__ == "__main__":
  main()
