import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from util.sql_helper import SQLHelper

if __name__ == "__main__":
    SQLHelper().create_table()