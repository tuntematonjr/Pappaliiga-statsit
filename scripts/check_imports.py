import traceback
import sys


def main():
    print('Python executable:', sys.executable)
    print('CWD:', sys.path[0])
    print('sys.path:')
    for p in sys.path:
        print('  ', p)
    try:
        import env_loader
        print('env_loader module loaded from:', getattr(env_loader, '__file__', '<builtin>'))
        print('load_env function present:', hasattr(env_loader, 'load_env'))
    except Exception as exc:
        print('Import failed:')
        print(type(exc).__name__, repr(exc))
        print('Traceback:')
        traceback.print_exc()


if __name__ == '__main__':
    main()
