# coding: utf-8

# This is a sample server which acts as a Redis server but uses Bitcask
# instead. It only implements GET and SET operations.

import socketserver


def _pack_value(value):
    return b'$' + bytes(str(len(value)), 'ascii') + b'\r\n' + value + b'\r\n'


class MyTCPHandler(socketserver.StreamRequestHandler):

    def _read_command(self):
        first = self.rfile.readline().rstrip()
        assert first.startswith(b'$')
        length = int(first[1:])
        data = self.rfile.readline()
        assert len(data) == length + 2
        return data[:-2]

    def handle(self):
        data = self.rfile.readline().rstrip()

        assert data.startswith(b'*')
        commands_to_read = int(data[1:])
        command = self._read_command()
        parameters = [self._read_command() for _ in range(commands_to_read - 1)]

        if command == b'SET':
            if len(parameters) != 2:
                # TODO: send the correct error
                self.wfile.write(b'-ERR\r\n')
            else:
                self.server._db[parameters[0]] = parameters[1]
                self.wfile.write(b'+OK\r\n')
        elif command == b'GET':
            key = parameters[0]
            try:
                value = self.server._db[parameters[0]]
            except KeyError:
                self.wfile.write(b'$-1\r\n')
            else:
                self.wfile.write(_pack_value(value))
        else:
            # TODO: send the correct error
            self.wfile.write(b'-ERR\r\n')


if __name__ == "__main__":
    HOST, PORT = "localhost", 9999

    import bitcask
    server = socketserver.TCPServer((HOST, PORT), MyTCPHandler)
    server._db = bitcask.Bitcask('mycask')
    server.serve_forever()
