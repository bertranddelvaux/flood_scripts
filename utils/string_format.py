def colorize_text(text, color='normal'):
    """
    Wrap a string with ANSI escape codes to change its color and/or style.

    Arguments:
    text -- The string to colorize
    color -- A string indicating the desired color and style. Valid values are 'normal', 'red', 'green', and 'bold'.

    Returns:
    The input string wrapped in ANSI escape codes to change its color and/or style.
    """
    colors = {
        'normal': '\033[0m',
        'red': '\033[31m',
        'green': '\033[32m',
        'bold': '\033[1m'
    }

    if color not in colors:
        raise ValueError(f"Invalid color '{color}'. Valid values are {', '.join(colors.keys())}.")

    return f"{colors[color]}{text}{colors['normal']}"
