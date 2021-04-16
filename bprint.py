reference = {
    '*b': '\033[1m',
    '*u': '\033[4m',
    '*r': '\033[91m',
    '*y': '\033[93m',
    '\*': '\033[0m',
}

def format_string(line):
    for k in reference.keys():
        line = line.replace(k, reference[k])
    return line

def bprint(content, text_color = None, text_format = None, tag = None):
    textcolor = {
        'purple': '\033[95m',
        'cyan': '\033[96m',
        'darkcyan': '\033[36m',
        'blue': '\033[94m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'red': '\033[91m',
    }

    textformat = {
        'bold': '\033[1m',
        'underline': '\033[4m',
    }

    end = '\033[0m'

    try:
        str(content)
        str_able = True
    except:
        str_able = False

    if isinstance(content, str) or str_able and not isinstance(content, list):
        _content = str(content)
    elif isinstance(content, list):
        _content = '\n'.join([x for x in content])

    if tag:
        tag = tag.upper()
        tag = f"{textformat['bold']}[{tag}] "
        if isinstance(content, list):
            tag += "\n"
        _content = tag + _content

    if text_color:
        if text_color in textcolor.keys():
            _content = textcolor[text_color] + _content
        else:
            raise KeyError(f'{text_color} is not available')

    if text_format:
        if text_format in textformat.keys():
            _content = textformat[text_format] + _content
        else:
            raise KeyError(f'{text_format} is not available')

    _content += end
    # _content = format_string(_content)
    print (_content)
