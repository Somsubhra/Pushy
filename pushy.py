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

    self.console_show_message("Starting Pushy push server")

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
      self.console_show_error(str(e))

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

    self.console_show_success("Pushy listening on port " + str(self.port))

    while True:

      read_sockets, write_sockets, error_sockets = select.select(self.connection_list, [], [])

      for sock in read_sockets:

        if sock == self.server_socket:

          # Accept incoming connections to server
          sockfd, addr = self.server_socket.accept()
          self.connection_list.append(sockfd)
          self.console_show_message("Client " + str(addr) + " connected to Pushy")

        else:

          # Process data from clients
          try:
            data = sock.recv(self.recv_buffer_size)
            if data:
              self.console_show_message("New message from" + str(sock.getpeername()))

              if self.is_command(data):
                self.exec_command(data, sock)

          except:
            self.console_show_message("Client " + str(addr) + " disconnected from Pushy")
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

    self.console_show_message("Executing " + command + " command with args " + str(args))

    commands = {
      'reg': self.register,
      'id': self.identify,
      'pub': self.publish,
      'sub': self.subscribe
    }

    if command in commands:
      commands[command](args, socket)
    else:
      self.console_show_error("Invalid command")


  # Register command
  def register(self, args, socket):

    if len(args) < 3:
      self.console_show_usage("/reg <id> <name> <pass>")
      return

    self.console_show_message("Registering " + str(args))

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
      self.console_show_success("Registered channel " + args[0])

    except Exception as e:
      db.rollback()
      self.console_show_error(str(e))

    finally:
      db.close()


  # Identify command
  def identify(self, args, socket):

    if len(args) < 2:
      self.console_show_usage("/id <channel_id> <password>")
      return

    self.console_show_message("Identifying " + str(args))

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
        self.console_show_error("Identification failure")
        return

      for row in channel:
        channel_id = row[0]
        self.identified_connections[socket] = str(channel_id)
        self.console_show_success("Channel " + str(channel_id) + " identified")

    except Exception as e:
      db.rollback()
      self.console_show_error(str(e))

    finally:
      db.close()


  # Publish to channel
  def publish(self, args, socket):

    if not self.is_identified_connection(socket):
      self.console_show_error("Need to be identified first")
      return

    if len(args) < 1:
      self.console_show_usage("/pub <your_message>")
      return

    publisher_id = self.identified_connections[socket]
    message = ' '.join(args)

    self.console_show_message("Publishing " + str(args))

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

      self.console_show_success("Published message to subscribers")

    except Exception as e:
      db.rollback()
      self.console_show_error(str(e))

    finally:
      db.close()


  # Subscribe to channel
  def subscribe(self, args, socket):

    if not self.is_identified_connection(socket):
      self.console_show_error("Need to be identified first")
      return

    if len(args) < 1:
      self.console_show_usage("/sub <publisher_id>")
      return

    self.console_show_message("Subscribing " + str(args))

    try:
      db = sqlite3.connect(self.db_name)
      db_cursor = db.cursor()

      query = '''
        INSERT INTO subscriber(publisher_id, subscriber_id)
        VALUES(?, ?)
      '''

      db_cursor.execute(query, (args[0], self.identified_connections[socket]))
      db.commit()
      self.console_show_success("Subscribed to channel " + args[0])

    except Exception as e:
      db.rollback()
      self.console_show_error(str(e))

    finally:
      db.close()


  # Send message to clients
  def message_clients(self, message, subscriber_ids, publisher_id):

    message = str(publisher_id) + ": " + message

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
    self.console_show_message("Ctrl+C caught, exiting")
    self.console_show_success("Pushy server exiting gracefully")
    sys.exit(0)


  # Show error message on console
  def console_show_error(self, message):
    print "\033[91m" + "Error: " + message + "\033[0m"


  # Show success message on console
  def console_show_success(self, message):
    print "\033[92m" + "Success: " + message + "\033[0m"


  # Show message on console
  def console_show_message(self, message):
    print "\033[94m" + "-- " + message + "\033[0m"


  # Show command usage on console
  def console_show_usage(self, message):
    print "\033[93m" + "Usage: " + message + "\033[0m"


# The main method of the program
def main():
  pushy_server = Pushy("0.0.0.0", 5000, 4096, 'pushy_data.db')
  pushy_server.run()


if __name__ == "__main__":
  main()
