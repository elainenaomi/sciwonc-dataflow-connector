class ConditionTree:
	left = None
	op = None
	right = None

	def __init__(self, left, op, right):
		self.left = left
		self.op = op
		self.right = right

	def convert_to_mongo_condition(self):
		operator_string = {
			"=": "$eq",
			">": "$gt",
			">=": "$gte",
			"<": "$lt",
			"<=": "$lte",
			"!=": "$ne",
			"in": "$in",
			"not in": "$nin",
			"or": "$or",
			"and": "$and"
		}

		if isinstance(self.left, ConditionTree):
			converted_left = self.left.convert_to_mongo_condition()
		else:
			converted_left = self.left

		if isinstance(self.right, ConditionTree):
			converted_right = self.right.convert_to_mongo_condition()
		else:
			converted_right = self.right

		if self.op in operator_string:
			if self.op == "and" or self.op == "or":
				return {operator_string[self.op]: [converted_left, converted_right]}
			else:
				return {converted_left: {operator_string[self.op]: converted_right}}
		else:
			return None

	def convert_to_cassandra_condition(self):
		if isinstance(self.left, ConditionTree):
			converted_left = self.left.convert_to_cassandra_condition()
		else:
			converted_left = "\"" + str(self.left) + "\""

		if isinstance(self.right, ConditionTree):
			converted_right = self.right.convert_to_cassandra_condition()
		else:
			right_as_string = str(self.right)
			if (self.right == right_as_string):
				converted_right = "\'" + self.right.replace("'", "''") + "\'"
			else:
				converted_right = right_as_string

		return converted_left + " " + self.op + " " + converted_right

	def convert_to_sql_condition(self):
		return self.convert_to_cassandra_condition()
