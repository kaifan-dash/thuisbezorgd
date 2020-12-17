class color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class bprint:
    def red(line):
        print(color.RED + str(line) + color.END)
    def blue(line):
        print(color.BLUE + str(line) + color.END)
    def yellow(line):
        print(color.YELLOW + str(line) + color.END)
    def green(line):
        print(color.GREEN + str(line) + color.END)
