HOST = "localhost"
PORT = "27017"
USER = ""
PASSWORD = ""
DATABASE = "google"
COLLECTION_INPUT = "ratio"
COLLECTION_OUTPUT = "mean_ratio"

ATTRIBUTES = ["event type", "ratio cpu memory"]
SORT = ["numline", "filepath"]
OPERATION_TYPE = "GROUP_BY_COLUMN"
COLUMN = "event type"
VALUE = "0"

INPUT_FILE = "ratio_cpu_memory.csv"
OUTPUT_FILE = "mean_ratio_cpu_memory.csv"
