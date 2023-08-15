import argparse
from lambdamoo_db.reader import load
from lambdamoo_db.writer import dump

# Initialize parser
parser = argparse.ArgumentParser()

# Add input (load) argument
parser.add_argument("--load", help="Path to the database to load. Default: 'mongoose.db'", default="./mongoose.db")

# Add output (dump) argument
parser.add_argument("--dump", help="Path to the file to dump to. Default: './mongoose.new'", default="./mongoose.new")

# Parse arguments
args = parser.parse_args()

# Load file
db = load(args.load)

# Dump to file
with open(args.dump, 'w', newline='\n') as fp:
    dump(db, fp)
