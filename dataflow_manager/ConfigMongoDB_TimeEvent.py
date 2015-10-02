HOST = "localhost"
PORT = "27017"
USER = ""
PASSWORD = ""
DATABASE = "google"
COLLECTION_INPUT = "tasks"
COLLECTION_OUTPUT = "task_time_event"

ATTRIBUTES = ["job ID", "task index","event type", "time"]
SORT = ["_id.numline", "_id.filepath"]
OPERATION_TYPE = "GROUP_BY_COLUMN"
COLUMN = ("job ID", "task index")
VALUE = [("3418400","0"),("3418400","1")]

INPUT_FILE = "task_events.csv"
OUTPUT_FILE = "time_task_events.csv"
