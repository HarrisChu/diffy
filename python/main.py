import sys

from nebula import Executor

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py [process|thread]")
        sys.exit(1)
    typ = sys.argv[1]
    executor = Executor(typ)
    print(executor.run())
    

if __name__ == '__main__':
    main()
    
