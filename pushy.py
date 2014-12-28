# Pushy push server
# Written by Somsubhra

# All imports
import socket
import select
import sqlite3

# The main method of the program
def main():
  
  # Setup database
  try:
    db = sqlite3.connect('pushy_data.db')
    db_cursor = db.cursor()
    query = '''
      CREATE TABLE IF NOT EXISTS channel (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      )
    '''
    db_cursor.execute(query)
    db.commit()
  except Exception as e:
    db.rollback()
  finally:
    db.close()
  
  # Socket descriptors
  connection_list = []
  recv_buffer = 4096
  port = 5000

  server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  server_socket.bind(("0.0.0.0", port))
  server_socket.listen(10)

  connection_list.append(server_socket)

  print "Pushy listening on port " + str(port)

  while True:

    read_sockets, write_sockets, error_sockets = select.select(connection_list, [], [])

    for sock in read_sockets:

      if sock == server_socket:
        # Accept incoming connections to server
        sockfd, addr = server_socket.accept()
        connection_list.append(sockfd)
        print "Client (%s %s) connected to Pushy" % addr
      else:
        # Process data from clients
        try:
          data = sock.recv(recv_buffer)
          if data:
            print "New message from" + str(sock.getpeername()) + ": " + data
        except:
          print "Client (%s, %s) disconnected from Pushy" % addr
          sock.close()
          connection_list.remove(sock)
          continue

  server_socket.close()

if __name__ == "__main__":
  main()
