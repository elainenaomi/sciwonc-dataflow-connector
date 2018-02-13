class DataObject(object):
    cursor = None
    config = None
    type = None

    def __init__(self, type, cursor,config):
        self.type = type
        self.cursor = cursor
        self.config = config

    def next(self):
        if self.type == "postgres":
            if self.config.OPERATION_TYPE == "DISTINCT":
                if self.cursor is None:
                    return None
                rows = self.cursor.fetchall()
                self.cursor = None
                return [str(a[0]) for a in rows]
            else:
                row = self.cursor.fetchone()
                #print row

                if row:
                    keys = tuple(self.config.ATTRIBUTES)
                    values = row
                    return {k: v for k, v in zip(keys, values)}
        elif self.type == "mongodb":
            if self.config.OPERATION_TYPE == "DISTINCT":
                rows = self.cursor
                self.cursor = None
                return rows
            else:
                row = next(self.cursor,None)
                #row = dict(row)
                return row

        return None
