import time

import click as cli
from columnar import columnar

from connector import TokenConnection, ResponseType, SingletonTableEntry
from connector import User


@cli.command()
@cli.option("--host", prompt="Enter your host", help="Address of your system to connect to.")
@cli.option("--port", default=2291, help="Port of system to connect to.")
@cli.option("--user", prompt="Enter your username", help="Your username to login.")
@cli.option("--password", prompt="Enter your password", help="The password of your user to login.")
def connect(host, port, user, password):
    connection = TokenConnection(f"{host}:{port}", User(user, password))

    print(f"Connecting to {host}...")

    try:
        connection.connect()
    except Exception as ex:
        print(f"Connection failed! Cause by {ex}")
        return

    print(f"Successfully connected.")

    while execute(connection, input("> ")):
        pass


def execute(connection, command):
    if command == "close" or command == "exit" or command == "leave":
        return False

    init = round(time.time() * 1000)
    response = connection.query(command.removeprefix("!"))
    took = round(time.time() * 1000) - init

    if command.startswith("!"):
        print(response.__str__())
    else:
        visualize(response)

    print(f"{took} ms")

    return True


def visualize(response):
    if response.type == ResponseType.ERROR:
        print(response.exception())
    elif response.type == ResponseType.SYNTAX_ERROR:
        print("Unknown syntax!")
    elif response.type == ResponseType.SUCCESS:
        print("Command successfully executed.")
    elif response.type == ResponseType.FORBIDDEN:
        print("You don't have the permissions to do that!")
    else:
        fields = response.structure()
        entries = response.entries()
        datas = []

        for entry in entries:
            if type(entry) is SingletonTableEntry:
                datas.append([entry.get()])
            else:
                data = []

                for field in fields:
                    data.append("null" if entry.is_null(field) else entry.get(field))

                datas.append(data)

        print(columnar(datas, headers=fields))


if __name__ == '__main__':
    connect()
