import os
import sys

# Resolve imports from the project root so shared modules are loaded consistently.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

def main():
    print("Running transformer...")

if __name__ == "__main__":
    main()
