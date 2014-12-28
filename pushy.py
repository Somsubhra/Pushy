# Pushy push server
# Written by Somsubhra

# All imports
import socket
import select
import sqlite3


class Pushy:


  # Constructor for the Pushy server
  def __init__(self, host, port, recv_buffer_size):

    self.host = host
    self.port = port
    self.recv_buffer_size = recv_buffer_size

    # Setup database
    try:
      self.db = sqlite3.connect('pushy_data.db')
      self.db_cursor = db.cursor()
      query = '''
        CREATE TABLE IF NOT EXISTS channel (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL
        )
      '''
      self.db_cursor.execute(query)
      self.db.commit()
    except Exception as e:
      self.db.rollback()
    finally:
      self.db.close()

    # Set up Pushy server's socket
    self.connection_list = []

    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server_socket.bind((self.host, self.port))
    self.server_socket.listen(10)

    self.connection_list.append(self.server_socket)


  # The main loop of Pushy server
  def run(self):
    print "Pushy listening on port " + str(self.port)

    while True:

      read_sockets, write_sockets, error_sockets = select.select(self.connection_list, [], [])

      for sock in read_sockets:

        if sock == self.server_socket:

          # Accept incoming connections to server
          sockfd, addr = self.server_socket.accept()
          self.connection_list.append(sockfd)
          print "Client (%s %s) connected to Pushy" % addr

        else:

          # Process data from clients
          try:
            data = sock.recv(self.recv_buffer_size)
            if data:
              print "New message from" + str(sock.getpeername()) + ": " + data

              if self.is_command(data):
                self.exec_command(data)

          except:
            print "Client (%s, %s) disconnected from Pushy" % addr
            sock.close()
            self.connection_list.remove(sock)
            continue

    self.server_socket.close()

  # Return whether the message is a command or not
  def is_command(self, message):
    return message[0] == '/'

  # Execute the command
  def exec_command(self, message):
    message = message.split()

    command = message[0]
    command = command[1:]

    args = []

    if len(message) > 1:
      args = message[1:]

    print "Executing " + command + " command with args " + str(args)

    commands = {
      'reg': self.register,
      'id': self.identify
    }

    if command in commands:
      commands[command](args)
    else:
      print "Invalid command"


  # Register command
  def register(self, args):
    print "Registering " + str(args)

  def identify(self, args):
    print "Identifying " + str(args)


# The main method of the program
def main():
  pushy_server = Pushy("0.0.0.0", 5000, 4096)
  pushy_server.run()


if __name__ == "__main__":
  main()
