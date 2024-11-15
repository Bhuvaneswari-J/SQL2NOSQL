import xml.etree.ElementTree as ET
from pymongo import MongoClient
import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Where
from sqlparse.tokens import Keyword, DML
import re


class Mediator:
    def __init__(self, db_metadata, convert):
        self.db_metadata = db_metadata
        self.convert = convert

    def intercept_query(self, sql_query, query_type):
        # Intercept SQL query and convert it to XML format
        xml_request = self.create_xml_request(sql_query, query_type)
        print("Query intercepted and converted to XML format.")

        # Send XML request to Convert for processing
        result = self.convert.process_query(xml_request)
        
        # Handle result formatting and send back to application
        return self.format_result(result)

    def create_xml_request(self, query, query_type):
        # Create XML structure for the intercepted query
        root = ET.Element("Request")
        ET.SubElement(root, "xmlns").text = "queryInterceptor"
        ET.SubElement(root, "method").text = "Intercepta"
        ET.SubElement(root, "query").text = query
        ET.SubElement(root, "queryType").text = query_type
        return ET.tostring(root, encoding="utf-8", method="xml")

    def format_result(self, result):
        # Format NoSQL result to relational format (simulate headers)
        formatted_result = {
            "headers": list(result[0].keys()) if result else [],
            "rows": [list(row.values()) for row in result]
        }
        return formatted_result


class Convert:
    def __init__(self, mongo_client, db_metadata):
        self.mongo_client = mongo_client
        self.db_metadata = db_metadata

    def process_query(self, xml_request):
        # Parse XML request and extract query data
        query_data = self.parse_xml_request(xml_request)
        sql_query = query_data['query']
        query_type = query_data['queryType']
        
        # Transform SQL query to NoSQL query
        nosql_query = self.translate_to_nosql(sql_query, query_type)

        # Execute the translated NoSQL query
        return self.execute_nosql_query(nosql_query, query_type)

    def parse_xml_request(self, xml_request):
        root = ET.fromstring(xml_request)
        return {
            "query": root.find("query").text,
            "queryType": root.find("queryType").text
        }

    def get_table_name(self, parsed_query):
        # Locate the table name by identifying the FROM clause
        from_seen = False
        for token in parsed_query.tokens:
            if from_seen:
                if isinstance(token, Identifier):
                    return token.get_real_name()
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        return identifier.get_real_name()
            if token.ttype is Keyword and token.value.upper() == "FROM":
                from_seen = True
        return None

    def get_where_clause(self, parsed_query):
        for token in parsed_query.tokens:
            if isinstance(token, Where):
                return token
        return None

    def parse_where_clause(self, where_clause):
        filter_dict = {}
        operator_mapping = {
            "=": "$eq", "!=": "$ne", ">": "$gt", "<": "$lt", ">=": "$gte", "<=": "$lte", "LIKE": "$regex"
        }
        tokens = [token for token in where_clause.tokens if not token.is_whitespace and token.value.upper() != "WHERE"]

        current_field = None
        current_operator = None

        for token in tokens:
            if isinstance(token, Identifier):
                current_field = token.get_real_name()
            elif token.ttype is Keyword and token.value.upper() in operator_mapping:
                current_operator = operator_mapping[token.value.upper()]
            elif token.ttype in (sqlparse.tokens.Literal.Number.Integer, sqlparse.tokens.Literal.Number.Float, sqlparse.tokens.Literal.String.Single):
                value = token.value.strip("'") if token.ttype == sqlparse.tokens.Literal.String.Single else float(token.value) if '.' in token.value else int(token.value)
                if current_operator and current_field:
                    if current_operator == "$regex":  # Handle LIKE queries
                        regex = value.replace("%", ".*").replace("_", ".")
                        filter_dict[current_field] = {current_operator: f"^{regex}$"}
                    else:
                        filter_dict[current_field] = {current_operator: value}
                    current_field = None
                    current_operator = None
        return filter_dict


    def translate_to_nosql(self, sql_query, query_type):
        parsed_query = sqlparse.parse(sql_query)[0]
        table_name = self.get_table_name(parsed_query) if query_type in ["SELECT", "DELETE", "UPDATE"] else re.search(r"INSERT INTO (\w+)", sql_query, re.IGNORECASE).group(1) if query_type == "INSERT" else None

        if not table_name:
            raise ValueError("Table name not found in query.")
        
        collection_name = self.db_metadata.get_collection_name(table_name)
        nosql_query = {"collection": collection_name, "operation": query_type}

        if query_type == "SELECT":
            where_clause = self.get_where_clause(parsed_query)
            nosql_query["filter"] = self.parse_where_clause(where_clause) if where_clause else {}

        elif query_type == "INSERT":
            nosql_query["data"] = self.parse_insert_values(sql_query)

        elif query_type == "UPDATE":
            where_clause = self.get_where_clause(parsed_query)
            nosql_query["update_data"], nosql_query["filter"] = self.parse_update_values(sql_query)

        elif query_type == "DELETE":
            where_clause = self.get_where_clause(parsed_query)
            nosql_query["filter"] = self.parse_where_clause(where_clause) if where_clause else {}

        print(f"Translated {query_type} to NoSQL query format: {nosql_query}")
        return nosql_query


    def parse_insert_values(self, sql_query):
        parsed = sqlparse.parse(sql_query)[0]
        tokens = parsed.tokens

        columns, values, values_section = [], [], False
        for token in tokens:
            if isinstance(token, IdentifierList):
                columns.extend([identifier.get_real_name() for identifier in token.get_identifiers()])
            elif isinstance(token, Identifier):
                columns.append(token.get_real_name())
            elif token.ttype is Keyword and token.value.upper() == "VALUES":
                values_section = True
            elif values_section and isinstance(token, sqlparse.sql.Parenthesis):
                for val in token.tokens:
                    if val.ttype in (sqlparse.tokens.Literal.Number.Integer, sqlparse.tokens.Literal.Number.Float, sqlparse.tokens.Literal.String.Single):
                        values.append(int(val.value) if val.ttype == sqlparse.tokens.Literal.Number.Integer else float(val.value) if val.ttype == sqlparse.tokens.Literal.Number.Float else val.value.strip("'"))
        if len(columns) != len(values):
            raise ValueError("Number of columns and values do not match in INSERT statement.")
        return dict(zip(columns, values))

    def parse_update_values(self, sql_query):
        parsed = sqlparse.parse(sql_query)[0]
        tokens = parsed.tokens

        update_data, filter_dict, set_seen = {}, {}, False

        for token in tokens:
            if set_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        field, value = self.extract_field_value(identifier)
                        update_data[field] = value
                elif isinstance(token, Identifier):
                    field, value = self.extract_field_value(token)
                    update_data[field] = value
                elif isinstance(token, Where):
                    filter_dict = self.parse_where_clause(token)
            if token.ttype is Keyword and token.value.upper() == "SET":
                set_seen = True

        return update_data, filter_dict

    def extract_field_value(self, identifier):
        tokens = identifier.tokens
        field, value = None, None
        for token in tokens:
            if isinstance(token, Identifier):
                field = token.get_real_name()
            elif token.ttype in (sqlparse.tokens.Literal.Number.Integer, sqlparse.tokens.Literal.Number.Float, sqlparse.tokens.Literal.String.Single):
                value = int(token.value) if token.ttype == sqlparse.tokens.Literal.Number.Integer else float(token.value) if token.ttype == sqlparse.tokens.Literal.Number.Float else token.value.strip("'")
        return field, value

    def execute_nosql_query(self, nosql_query, query_type):
        database_name = "migrated_db"
        collection = self.mongo_client[database_name][nosql_query["collection"]]
        filter_criteria = nosql_query.get("filter", {})

        if query_type == "SELECT":
            return list(collection.find(filter_criteria))
        elif query_type == "INSERT":
            collection.insert_one(nosql_query["data"])
            return [{"status": "inserted"}]
        elif query_type == "UPDATE":
            collection.update_many(filter_criteria, {"$set": nosql_query["update_data"]})
            return [{"status": "updated"}]
        elif query_type == "DELETE":
            collection.delete_many(filter_criteria)
            return [{"status": "deleted"}]
        else:
            raise ValueError(f"Unsupported query type: {query_type}")


class DatabaseMetadata:
    def __init__(self, table_collection_mapping):
        self.table_collection_mapping = table_collection_mapping

    def get_collection_name(self, table_name):
        return self.table_collection_mapping.get(table_name)


# Database metadata setup
table_collection_mapping = {
    "users": "users",
    "products": "products",
    "orders": "orders"
}
db_metadata = DatabaseMetadata(table_collection_mapping)

# MongoDB client setup
mongo_client = MongoClient("mongodb://localhost:27017/")
convert = Convert(mongo_client, db_metadata)
mediator = Mediator(db_metadata, convert)

# Sample query execution
sql_query = "SELECT * FROM users WHERE age > 25"
nosql_result = mediator.intercept_query(sql_query, "SELECT")
print("NoSQL Result:", nosql_result)
