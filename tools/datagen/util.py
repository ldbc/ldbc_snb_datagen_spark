import argparse
import sys


class KeyValue(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split('=')
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value


def ask_continue(message):
    print(message)
    resp = None
    inp = input("Continue? [Y/N]:").lower()
    while resp is None:
        if inp == 'y' or inp == 'yes':
            resp = True
        elif inp == 'n' or inp == 'no':
            resp = False
        else:
            inp = input("Please answer yes or no:").lower()
    return resp


def split_passthrough_args():
    args = sys.argv[1:]
    try:
        sep = args.index('--')
        return args[:sep], args[sep+1:]
    except ValueError:
        return args, []
