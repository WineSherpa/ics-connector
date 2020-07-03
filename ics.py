import cProfile
import postgresql


class KeyedList(object):
	def __init__(self, l, n):
		self.list = l
		d = {}
		for i in range(len(n)):
			d[n[i]] = i
		self.names = d
	
	def __getitem__(self, name):
		return self.list[self.names[name]]
	
	def __str__(self):
		return f"KeyedList names:{self.names}, list:{self.list}"

class VisionItem(object):
	__slots__ = [
		'id',
		'descr',
		'category',
		'department',
		'subdepartment',
		'vendor_name',
		'notes'
	]
	def __init__(self, **kwargs):
		for slot in self.__slots__:
			if slot not in kwargs:
				raise TypeError(f"Parameter named {slot} not supplied")
			setattr(self, slot, kwargs[slot])
	
	def __str__(self):
		s = "VisionItem #"
		for slot in self.__slots__:
			s += f" {slot}: {str(self.__slots__[slot])}"
		return s

class VisionTransaction(object):
	__slots__ = ['id', 'timestamp', 'total', 'tax', 'lines']
	def __init__(self, identifier, timestamp, lines):
		self.id = identifier
		self.timestamp = timestamp

		self.total = 0
		self.tax = 0
		self.lines = []
		for line in lines:
			self.total += line["price"]
			self.tax += line["tax"]
			self.lines.append(line)
	
	def __str__(self):
		s = "VisionTransaction #"
		for slot in self.__slots__:
			s += f" {slot}: {str(self.__getattribute__(slot))}"
		return s

# VisionTransactionLine
# item number
# quantity
# line price
# line tax		



class VisionDB(object):
	def __init__(self, hostname, username, password, database, port = "5432"):
		url = "pq://"+username+":"+password+"@"+hostname+":"+port+"/"+database
		self.db = postgresql.open(url)
		self.closed = False
		
		self.prepared = {}

		self.items = {}
		

	def __del__(self):
		self.close()

	def prep_query(self, tag, query):
		if self.closed:
			raise ConnectionError()
		if tag not in self.prepared:
			self.prepared[tag] = self.db.prepare(query)
		return self.prepared[tag]

	def close(self):
		if not self.closed:
			self.db.close()
			self.closed = True
		


	def getItem(self, item_num, force=False):
		if item_num in self.items and force is False:
			return self.items[item_num]
		query = """
			SELECT 
				item.item_num,
				item.descr,
				sub_department.description as subdepartment,
				department.description as department,
				department_group.name1 as category
			FROM item
				join sub_department on (item.deptsubdept = sub_department.deptsubdept)
				join department on (sub_department.dept = department.dept)
				join department_group on (department.dept_cat = department_group.dept_group )
			WHERE item_num = $1
		"""
		get_item = self.prep_query("get_item", query)
		i = get_item.first(item_num)
		item = VisionItem(
			id = i["item_num"],
			descr = i["descr"],
			category = i["category"],
			department = i["department"],
			subdepartment = i["subdepartment"]
		)
		return item
	
	def getItems(self, deleted=False):
		query = """
			SELECT 
				item.item_num,
				item.descr,
				sub_department.description as subdepartment,
				department.description as department,
				department_group.name1 as category,
				pri_vndr as vendor_name,
				item_extra.notes as notes
			FROM item
				join sub_department on (item.deptsubdept = sub_department.deptsubdept)
				join department on (sub_department.dept = department.dept)
				join department_group on (department.dept_cat = department_group.dept_group )
				left outer join item_extra on (item.item_num = item_extra.item_num)
			WHERE (deleted_flag = 'N' OR $1 = true)
		"""
		get_items = self.prep_query("get_items", query)
		items = get_items(deleted)
		ret = []
		
		for i in items:
			item = VisionItem(
				id = i["item_num"],
				descr = i["descr"],
				category = i["category"],
				department = i["department"],
				subdepartment = i["subdepartment"],
				vendor_name = i["vendor_name"],
				notes = i["notes"]
			)
			ret.append(item)
		return ret


	def getTransactions(self, start=None, end=None, on=None):
		start_clause = "and the_time::date >= '"+start+"'::date" if start is not None else ""
		end_clause = "and the_time::date <= '"+end+"'::date" if end is not None else ""
		on_clause = "and the_time::date = '"+on+"'::date" if on is not None else ""
		select = f"""
			select
				unique_id,
				item_num,
				retail,
				wholesale as cost,
				the_type as type,
				the_time as timestamp,
				isfm_id as transaction_id,
				num_units,
				(statetax+countytax+citytax) as tax
			from items_sold_final"""
		select_count = f"""
			select
				count(*)
			from items_sold_final"""
		where = f"""
			where
				void_item_flag != 'Y'
				and void_sale_flag != 'Y'
				and the_type !~ '[TSRL]'
				{start_clause}
				{end_clause}
				{on_clause}
			order by isfm_id desc;"""
		
		query = select + where
		count_query = select_count + where
		# get_transaction_lines = self.prep_query("get_transaction_lines", query)
		transaction_lines_count = self.db.prepare(count_query)()
		
		get_transaction_lines = self.db.prepare(query)

		def unwind(lists):
			for l in lists:
				for row in l:
					yield row

		transaction_lines = unwind(get_transaction_lines.chunks())

		
		
		def apply(lists, keys):
			for l in lists:
				yield KeyedList(l, keys)
		
		transaction_lines = apply(transaction_lines, get_transaction_lines.column_names)
		# print(transaction_lines)

		def transaction_lines_group(lines):
			group = []
			group_id = None
			for line in lines:
				if group_id is None:
					group_id = line["transaction_id"]
				if line["transaction_id"] != group_id:
					group_id = line["transaction_id"]
					yield group
					group = []
				group.append(line)
			if len(group) != 0:
				yield group

		
		def transactions(transaction_groups):
			for group in transaction_groups:
				lines = []
				transaction_id = group[0]["transaction_id"]
				transaction_timestamp = group[0]["timestamp"]

				for line in group:
					if "A" in line["type"]: # line is an item
						# print(line)
						lines.append({
							"id": line["unique_id"],
							"item_num": line["item_num"],
							"quantity": line["num_units"],
							"price": line["retail"],
							"tax": line["tax"],
							"cost": line["cost"]
						})
				yield VisionTransaction(transaction_id, transaction_timestamp, lines)
				lines = []

		return transactions(transaction_lines_group(transaction_lines))


if __name__ == "__main__":
	from dotenv import load_dotenv
	load_dotenv()
	import os

	username = os.getenv("username")
	password = os.getenv("password")
	database = os.getenv("database")
	hostname = os.getenv("hostname")

	import unittest
	class ICSTest(unittest.TestCase):
		def setUp(self):
			self.vdb = VisionDB(username=username, password=password, hostname=hostname, database="ICS")

		def test_get_item(self):
			item = self.vdb.getItem("24090")
			self.assertEqual(item.descr, "ZE TEST ITEM")

		def test_get_items(self):
			items = self.vdb.getItems()
			
		
		def test_get_transactions_on_02_18_2020(self):
			transactions = self.vdb.getTransactions(on="02/18/2020")
			total = 0
			tax = 0
			for transaction in transactions:
				total += float(transaction.total)
				tax += float(transaction.tax)
			self.assertAlmostEqual(total, 2328.21, 2, "Total sales for 02/18/2020 not calculated correctly")
			self.assertAlmostEqual(tax, 154.40, 2, "Total tax for 02/18/2020 not calculated correctly")

		# These tests dont match the built in sales report because
		# some transactions straddle the month boundary 
		# and this system handles the date differently
		def test_get_transactions_from_02_01_2020_to_02_29_2020(self):
			transactions = self.vdb.getTransactions(start="02/01/2020", end="02/29/2020")
			total = 0
			tax = 0
			for transaction in transactions:
				total += float(transaction.total)
				tax += float(transaction.tax)
			self.assertAlmostEqual(total, 137135.61, 2, "Total sales for Feb 2020 not calculated correctly")
			self.assertAlmostEqual(tax, 9019.02, 2, "Total tax for Feb 2020 not calculated correctly")
		
		def test_get_transactions_from_01_01_2020_to_01_31_2020(self):
			transactions = self.vdb.getTransactions(start="01/01/2020", end="01/31/2020")
			total = 0
			tax = 0
			for transaction in transactions:
				total += float(transaction.total)
				tax += float(transaction.tax)
			self.assertAlmostEqual(total, 111376.62, 2, "Total sales for Jan 2020 not calculated correctly")
			self.assertAlmostEqual(tax, 7344.38, 2, "Total tax for Jan 2020 not calculated correctly")

		def tearDown(self):
			self.vdb.close()

	unittest.main()
