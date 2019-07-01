import calendar
import collections
from time import strptime
from six import string_types
from lxml import etree
from itertools import chain
import codecs
import ujson
from logging import getLogger, StreamHandler, FileHandler, Formatter, DEBUG


def fast_iter(context, func):
    for event, elem in context:
        func(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context


def write_to_json(filename, data):
    """データをインデントありのjson形式で保存

    Args:
        filename(str): 保存するファイルのパス
        data(dict): 保存するdictデータ
    """
    with codecs.open(filename, mode='w', encoding='utf8', errors='ignore') as f:
        ujson.dump(data, f, ensure_ascii=False, indent=2)


def load_json(filename):
    """指定したjsonファイルを読み込む

    Args:
        filename(str): 読み込むファイルのパス

    Returns:
        data(dict): 読み込んだjsonの中身．ファイルが存在しない場合はNone．
    """
    try:
        with codecs.open(filename, mode='r', encoding='utf8', errors='ignore') as f:
            data = ujson.load(f)
            return data
    except FileNotFoundError:
        print(f'FileNotFound:"{filename}" そんなファイルないよ')
        return None


def make_logger(log_name, filename, mode='a',
                formatter='%(asctime)s - %(name)s - %(levelname)5s - %(message)s'):
    # set up logger
    logger = getLogger(log_name)
    logger.setLevel(DEBUG)

    # set up handler
    handler_format = Formatter(formatter)
    file_handler = FileHandler(filename, mode)
    file_handler.setLevel(DEBUG)
    file_handler.setFormatter(handler_format)

    # add handler
    logger.addHandler(file_handler)

    return logger
