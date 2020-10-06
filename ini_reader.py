import configparser
import sys

if __name__ == '__main__':
    ini = sys.argv[1]
    section = sys.argv[2]
    variable = sys.argv[3]

    cfg = configparser.ConfigParser()
    cfg.read(ini)

    print(cfg[section][variable])