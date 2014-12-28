# Pushy example client
# Written by Somsubhra
# (You can also use telnet)

# All imports
import socket
import select
import string
import sys

 # Message prompt
def prompt():
  sys.stdout.write(">")
  sys.stdout.flush()

# The main method of the program
def main():
  if(len(sys.argv) < 3) :
    print 'Usage : python client.py <hostname> <port>'
    sys.exit()

  host = sys.argv[1]
  port = int(sys.argv[2])

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.settimeout(2)

  # Connect to Pushy server
  try :
    s.connect((host, port))
  except :
    print 'Unable to connect'
    sys.exit()

  print 'Connected to Pushy server. Start sending messages\n'
  prompt()

  while True:

    socket_list = [sys.stdin, s]

    # Get the list sockets which are readable
    read_sockets, write_sockets, error_sockets = select.select(socket_list , [], [])

    for sock in read_sockets:

      # Incoming message from Pushy server
      if sock == s:
        data = sock.recv(4096)
        if not data :
          print '\nDisconnected from Pushy server'
          sys.exit()
        else :
          sys.stdout.write(data)
          prompt()

      else :
        # Send message to Pushy server
        msg = sys.stdin.readline()
        s.send(msg)
        prompt()

if __name__ == "__main__":
  main()
