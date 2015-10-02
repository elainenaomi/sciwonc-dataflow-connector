HOST = "localhost"
PORT = "27017"
USER = ""
PASSWORD = ""
DATABASE = "google"
COLLECTION_INPUT = "tasks"
COLLECTION_OUTPUT = "ratio"

FIRST_ITEM = {"numline":1, "filepath":"task_google.csv"}
LAST_ITEM = {"numline":19, "filepath":"task_google.csv"}
ATTRIBUTES = ["job ID", "task index", "event type","time", "memory request","CPU request"]
SORT = ["numline", "filepath"]
OPERATION_TYPE = "UNIT"

INPUT_FILE = "task_google.csv"
OUTPUT_FILE = "ratio_cpu_memory.csv"
