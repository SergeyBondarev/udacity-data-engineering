from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData


DEFAULT_CONNECTION_STRING = 'postgresql://student:student@127.0.0.1/sparkifydb'


def create_graph(connection_string=DEFAULT_CONNECTION_STRING):
    graph = create_schema_graph(metadata=MetaData(connection_string))
    return graph


def export_graph(graph, filename, file_type='png'):
    if file_type == 'png':
        graph.write_png(filename)
    else:
        raise NotImplementedError


def main():
    graph = create_graph()
    export_graph(graph, 'schema.png')


if __name__ == "__main__":
    main()