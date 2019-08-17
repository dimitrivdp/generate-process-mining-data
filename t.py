"""Test file for sys.argv"""

import sys

print(len(sys.argv))

for a in sys.argv:
    print(a)
    print(type(a))

print('Done')
