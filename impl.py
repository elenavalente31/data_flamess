# Implementation of the classes we see in the UML:

class IdentifiableEntity:
    def __init__(self, identifiers):
        self.id = set() 
        for identifier in identifiers:
            self.id.add(identifier)

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result

class Journal(IdentifiableEntity):
    def __init__(self, identifiers, title, languages, seal: bool, licence, apc: bool, publisher=None, categories=None, areas=None):
        super().__init__(identifiers) 
        self.title = title
        self.languages = languages   
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc
        self.categories = categories or []
        self.areas= areas or []     


    # Methods
    def getTitle(self):
        return self.title

    def getLanguages(self):
        return sorted(self.languages)  # Returns a sorted list

    def getPublisher(self):
        return self.publisher

    def hasDOAJSeal(self):
        return self.seal

    def getLicence(self):
        return self.licence

    def hasAPC(self):
        return self.apc  
    
    # Methods for relationships
    def getCategories(self):
        return self.categories
    
    def getAreas(self):
        return self.areas
    

    
class Category(IdentifiableEntity):
    def __init__(self, identifiers, category=None, quartile=None): 
        super().__init__(identifiers)
        self.category = category  
        self.quartile = quartile  

    def getQuartile(self):
        return self.quartile



class Area(IdentifiableEntity):
    def __init__(self, identifiers):
        super().__init__(identifiers)




# Data loading management (JSON -> SQL)

class Handler():
    
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl): # creates the dbPath attribute
        self.dbPathOrUrl = pathOrUrl
        return True



class UploadHandler(Handler):

    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        pass



# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# MARI

# From JSON to relational DB

import sqlite3
import json


class CategoryUploadHandler(UploadHandler):
    
    def __init__(self):
        super().__init__()
    
    def pushDataToDb(self, path):
        db_path = self.getDbPathOrUrl()


        def sanitize(value):
            return value.replace("'", "''").replace(';', ',') if value else ''
        
        conn = None

# Database connection
    
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        try:

            # Table creation
            # -- Main Journal table
            cursor.execute('''CREATE TABLE IF NOT EXISTS Journal (
            internal_id TEXT PRIMARY KEY);''')

            # -- Alternative identifiers for a Journal
            cursor.execute('''CREATE TABLE IF NOT EXISTS JournalIdentifier (
            journal_id TEXT NOT NULL,
            identifier TEXT NOT NULL,
            PRIMARY KEY (journal_id, identifier),
            FOREIGN KEY (journal_id) REFERENCES Journal(internal_id));''')

            # -- Categories (e.g., Hematology, Medicine, etc.)
            cursor.execute('''CREATE TABLE IF NOT EXISTS Category (
            category_id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            quartile TEXT NOT NULL,
            UNIQUE(category, quartile)
            );''') # UNIQUE(category, quartile) means that two different rows 
            # in the Category table cannot have the same values simultaneously 
            # in the category and quartile columns.

            # -- Areas (e.g., Computer Science, Medicine, etc.)
            cursor.execute('''CREATE TABLE IF NOT EXISTS Area (
            area_id TEXT PRIMARY KEY,
            area TEXT NOT NULL
            );''')

            # -- Many-to-many relationship: 
            cursor.execute('''CREATE TABLE IF NOT EXISTS HasCategory (
            journal_id TEXT NOT NULL,
            category_id TEXT NOT NULL,
            PRIMARY KEY (journal_id, category_id),
            FOREIGN KEY (journal_id) REFERENCES Journal(internal_id),
            FOREIGN KEY (category_id) REFERENCES Category(category_id)
            );''')

            # -- Many-to-many relationship: 
            cursor.execute('''CREATE TABLE IF NOT EXISTS HasArea (
            journal_id TEXT NOT NULL,
            area_id TEXT NOT NULL,
            PRIMARY KEY (journal_id, area_id),
            FOREIGN KEY (journal_id) REFERENCES Journal(internal_id),
            FOREIGN KEY (area_id) REFERENCES Area(area_id)
            );''')


            # JSON LOADING

            # File reading:
            
            with open(path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                    

            # --- Retrieving existing counters --- 
            # it is essential to retrieve the last existing ID in the database before starting to generate new IDs. 
            # This ensures that the new IDs are unique and do not overlap with existing ones.

            cursor.execute("SELECT IFNULL(MAX(CAST(SUBSTR(internal_id, INSTR(internal_id, '-') + 1) AS INTEGER)), -1) FROM Journal")
            journal_counter = cursor.fetchone()[0] + 1
            
            cursor.execute("SELECT IFNULL(MAX(CAST(SUBSTR(category_id, INSTR(category_id, '-') + 1) AS INTEGER)), -1) FROM Category")
            cat_counter = cursor.fetchone()[0] + 1

            cursor.execute("SELECT IFNULL(MAX(CAST(SUBSTR(area_id, INSTR(area_id, '-') + 1) AS INTEGER)), -1) FROM Area")
            area_counter = cursor.fetchone()[0] + 1
            # ----------------------------------------

            for journal_entry in json_data:
            # Checks whether the journal already exists based on one of its unique identifiers (e.g., ISSN/EISSN).
            # This is important to avoid duplicates:
            # e.g. the same journal appears in multiple JSON files or in subsequent executions.
            # It is assumed that 'identifiers' is present in the JSON and that at least one is sufficient for uniqueness.
                journal_identifiers = journal_entry.get('identifiers', []) # Extracts the list of identifiers from the JSON
                existing_journal_id = None # Initializes the variable to store the ID of the existing journal.
                
                # Checks ALL identifiers to see if the journal already exists in the database.
                if journal_identifiers: # Obviously there are identifiers to check because having identifiers is mandatory, but this if is added for safety.
                    for identifier_to_check in journal_identifiers: # Iterates over each identifier in the list.
                        cursor.execute('''
                            SELECT Journal.internal_id
                            FROM Journal
                            JOIN JournalIdentifier ON Journal.internal_id = JournalIdentifier.journal_id
                            WHERE JournalIdentifier.identifier = ?
                        ''', (identifier_to_check,)) # This query looks for a journal with that identifier
                        result = cursor.fetchone() # Retrieves the first row of the query result. It will be `None` if no match is found, otherwise a tuple containing the journal's internal_id (e.g., ('journal-123',)).
                        if result: # If the query found a match (i.e., `result` is not `None`), a Journal with that id is found:
                            existing_journal_id = result[0] # Assign the internal_id of the found journal to a variable
                            break # the loop over identifiers is interrupted (break). It is not necessary to check other identifiers for this journal since a match has already been found.

                if existing_journal_id: # If an internal_id of an existing journal was found:
                    current_journal_id = existing_journal_id # The current internal_id of the journal to process is the existing one.

                else: # Otherwise, if `existing_journal_id` is still `None` (i.e., the journal does not yet exist in the database).
                    current_journal_id = f'journal-{journal_counter}' # Create a new internal_id for the journal
                    try: # Start a try block to handle potential integrity errors
                        cursor.execute("INSERT INTO Journal (internal_id) VALUES (?)", (current_journal_id,)) # Attempt to insert the new journal with its internal_id (e.g., journal-random number)
                        journal_counter += 1 # Increment the counter for the next new journal

                    except sqlite3.IntegrityError: # Catch the exception if an integrity error occurs (e.g., unexpected duplicate ID)
                        print(f"Warning: Journal ID '{current_journal_id}' already exists (unexpected). Skipping insertion of this journal.") # Print a warning
                        continue # Skip processing the current `journal_entry` and move to the next.
                    # N.B. This clause catches an IntegrityError that may occur if, for some unexpected reason, 
                    # the current_journal_id already exists (although the counter system should prevent it, 
                    # it is good defensive practice). In that case, a warning is printed and the loop moves to the next journal_entry (continue) 
                    # without further processing the current one, avoiding a crash.

                # Once the code has determined the current_journal_id for the JSON entry being processed, it proceeds to associate to that journal_id (journal-number according to the counter) all identifiers (i.e., ISSN and EISSN) present in the journal_identifiers list in the JSON.

                # Inserting Identifiers
                for identifier in journal_identifiers: # Iterates over each identifier of the current journal
                    cursor.execute('''
                        INSERT OR IGNORE INTO JournalIdentifier (journal_id, identifier)
                        VALUES (?, ?)
                    ''', (current_journal_id, identifier)) # Inserts the identifier associated with the journal. `INSERT OR IGNORE` avoids duplicates if the identifier already exists for that journal.
                
                # Inserting Categories and HasCategory association
                for category_data in journal_entry.get('categories', []): # Iterates over each item in the 'categories' list in the JSON
                    category_name = sanitize(category_data.get('id', '')).strip() # Extracts and sanitizes the category name 
                    quartile = category_data.get('quartile', '').strip() # Extracts the category's quartile

                    if not category_name: # Skip if the category name is empty after sanitization.
                        continue

                    cursor.execute("SELECT category_id FROM Category WHERE category = ? AND quartile = ?", (category_name, quartile)) # Runs a query to see if a category with the same name and quartile already exists in the database. Checking both fields is essential, as the same category may have different quartiles.
                    existing_cat = cursor.fetchone() # Retrieves the first row of the result.
                    
                    if existing_cat: # If the category (with that quartile) already exists:
                        cat_id_to_use = existing_cat[0] # Use the existing category_id (cat-number) of the category.
                    else: # If the category (with that quartile) does not exist:
                        cat_id_to_use = f'cat-{cat_counter}' # Create a new category_id for the category.

                        try: # Start a try block to handle potential integrity errors
                            cursor.execute('''
                                INSERT INTO Category (category_id, category, quartile)
                                VALUES (?, ?, ?)
                            ''', (cat_id_to_use, category_name, quartile)) # Attempt to insert the new category
                            cat_counter += 1 # Increment the counter for the next new category

                        except sqlite3.IntegrityError: # Catch the exception if an integrity error occurs
                            print(f"Warning: Category ID {cat_id_to_use} or category/quartile combination already exists (unexpected). Skipping association for this category.")

                    # Inserting into HasCategory table to associate the journal with the category.
                    cursor.execute('''
                        INSERT OR IGNORE INTO HasCategory (journal_id, category_id)
                        VALUES (?, ?)
                    ''', (current_journal_id, cat_id_to_use)) # `INSERT OR IGNORE` avoids duplicates if the association already exists.
                
                # Inserting Areas and HasArea association
                for area_name_raw in journal_entry.get('areas', []): # Iterates over each item in the 'areas' list in the JSON.
                    if isinstance(area_name_raw, str): # Checks if the area name is a string.
                        # Here we use isinstance unlike how we handled categories
                        # The difference in handling lies in the expected structure of the elements in the JSON lists:
                        # While for categories, we expect a list of dictionaries, for areas, we expect a list of strings. 
                        safe_area_name = sanitize(area_name_raw.strip()) # Cleans the area name.
                        if not safe_area_name: # If the area name is not a valid string, i.e., it's an empty string or became empty after sanitization:
                            continue
                    else: # otherwise, if the area name is not a string:
                        continue # resume the loop. The continue stops the current iteration and moves to the next element. 

                   
                    cursor.execute("SELECT area_id FROM Area WHERE area = ?", (safe_area_name,)) # Searches for an existing area with the same name.
                    existing_area = cursor.fetchone() # Retrieves the first row of the result.
                    
                    if existing_area: # If the area already exists:
                        area_id_to_use = existing_area[0] # Use the existing area_id (area-number) of the area.
                    else: # If the area does not exist:
                        area_id_to_use = f'area-{area_counter}' # Create a new area_id (area-number) for the area.

                        try: # Start a try block to handle potential integrity errors.
                            cursor.execute('''
                                INSERT INTO Area (area_id, area)
                                VALUES (?, ?)
                            ''', (area_id_to_use, safe_area_name)) # Attempt to insert the new area.
                            area_counter += 1 # Increment the counter for the next new area.

                        except sqlite3.IntegrityError: # Catch the exception if an integrity error occurs.
                            print(f"Warning: Area ID '{area_id_to_use}' or area already exists (unexpected). Skipping association for this area.") # Print a warning.
                            continue # Skip association for this area and move to the next.
                    
                    # Inserting into HasArea table to associate the journal with the area.
                    cursor.execute('''
                        INSERT OR IGNORE INTO HasArea (journal_id, area_id)
                        VALUES (?, ?)
                    ''', (current_journal_id, area_id_to_use)) # `INSERT OR IGNORE` avoids duplicates if the association already exists.
                        
            # Committing changes to the database.
            conn.commit()
            return True
            #print(f"Data successfully loaded from {path} into database {db_path}.")

        except sqlite3.Error as e: # Catch any SQLite-specific error
            print(f"Database error during loading: {e}") 
            conn.rollback() # Perform a rollback of all operations in case of error to maintain database integrity.
        
        # Finally block that is always executed, regardless of whether an error occurred or not.
        finally:        
            conn.close()



# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# CRISTINA


import pandas as pd


class QueryHandler(Handler):
    def __init__(self):
        super().__init__()
       
    def getById(self, id):
        pass


class CategoryQueryHandler(QueryHandler):
    def getById(self, identifier: str) -> pd.DataFrame:
        """
        Given an identifier (e.g., ISSN), returns a DataFrame containing
        all data associated with the journal, with categories and areas
        aggregated as lists of strings.
        - internal_id
        - identifier (all IDs concatenated)
        - category (list of categories)
        - quartile (list of quartiles)
        - area (list of areas)

        Returns:
            pd.DataFrame: A DataFrame with a single row representing the journal.
        """
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Find the internal_id of the journal associated with the identifier
            cursor.execute('''
                SELECT journal_id FROM JournalIdentifier WHERE identifier = ?
            ''', (identifier,))
            row = cursor.fetchone()
            if not row:
                return pd.DataFrame() # if no journal is found, return an empty DataFrame

            journal_id = row[0] # we extract the journal_id from the fetched row

            # Retrieve all identifiers for that journal, in insertion order
            cursor.execute('''
                SELECT identifier
                FROM JournalIdentifier
                WHERE journal_id = ?
                ORDER BY rowid
            ''', (journal_id,)) # we get all identifiers for the journal that was found
            identifiers = [r[0] for r in cursor.fetchall()]
            identifiers_str = '; '.join(identifiers)

            # Query for category and area
            query = '''
            SELECT
                J.internal_id,
                C.category,
                C.quartile,
                A.area
            FROM Journal AS J
            LEFT JOIN HasCategory AS HC ON J.internal_id = HC.journal_id
            LEFT JOIN Category AS C ON HC.category_id = C.category_id
            LEFT JOIN HasArea AS HA ON J.internal_id = HA.journal_id
            LEFT JOIN Area AS A ON HA.area_id = A.area_id
            WHERE J.internal_id = ?
            '''  # query to retrieve journal, category and area data
            df = pd.read_sql_query(query, conn, params=(journal_id,))

            if df.empty:
                return pd.DataFrame({
                    'internal_id': [journal_id],
                    'identifier': [identifiers_str],
                    'category': [[]],
                    'quartile': [[]],
                    'area': [[]]
                }) #if no category/ area data exists, return a DataFrame with empty lists for those columns

            # Create (category, quartile) pairs to maintain alignment
            df['category_quartile'] = list(zip(df['category'], df['quartile']))

            # Aggregation
            aggregated = df.groupby('internal_id').agg({
                'category_quartile': lambda x: list({(cat, q) for cat, q in x if cat is not None}),
                'area': lambda x: list(x.dropna().unique())
            }).reset_index() # we group data by journal ID and aggregate categories/quartiles and areas into unique lists

            # Extract separate lists from category_quartile
            aggregated['category'] = aggregated['category_quartile'].apply(lambda x: [cat for cat, _ in x]) # separation of categories from the combined tuples
            aggregated['quartile'] = aggregated['category_quartile'].apply(lambda x: [q for _, q in x]) # separation of quartiles from the combined tuples

            # Insert identifier and remove the temporary column
            aggregated.insert(1, 'identifier', identifiers_str)
            aggregated.drop(columns=['category_quartile'], inplace=True) # remove the temporary combined column

            return aggregated

        except sqlite3.Error as e:
            print(f"Database error in getById: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()


    def getAllCategories(self) -> pd.DataFrame:
        """
        Returns a data frame containing all the distinct category names included in the database.

        Returns:
            pd.DataFrame: A DataFrame with a 'category' column.
        """
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            query = "SELECT DISTINCT category FROM Category"  # query to retrieve all unique category names
            df = pd.read_sql_query(query, conn)
            return df
        except sqlite3.Error as e:
            print(f"Database error in getAllCategories: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()


    def getAllAreas(self) -> pd.DataFrame:
        """
        Returns a data frame containing all the distinct area names included in the database.

        Returns:
            pd.DataFrame: A DataFrame with an 'area' column.
        """
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            query = "SELECT DISTINCT area FROM Area" # query to get all unique area names
            df = pd.read_sql_query(query, conn)
            return df
        except sqlite3.Error as e:
            print(f"Database error in getAllAreas: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()


    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame:
        """
        Returns a DataFrame containing all categories with the specified quartiles.

        Args:
            quartiles (set[str]): A set of quartiles (e.g., {'Q1', 'Q2'}) to filter by.
                                   If empty, returns all categories with their quartiles.

        Returns:
            pd.DataFrame: A DataFrame with 'category' and 'quartile' columns.
        """
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)

            base_query = "SELECT category, quartile FROM Category"  # query to select categories and quartiles

            if not quartiles:
                df = pd.read_sql_query(base_query, conn)
            else:
                placeholders = ','.join('?' * len(quartiles)) # we create placeholders for the SQL IN clause
                query = f"{base_query} WHERE quartile IN ({placeholders})" # we add a WHERE to filter by specified quartiles
                df = pd.read_sql_query(query, conn, params=list(quartiles))

            return df
        except sqlite3.Error as e:
            print(f"Database error in getCategoriesWithQuartile: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()


    def getCategoriesAssignedToAreas(self, area_names: set[str]) -> pd.DataFrame:
        """
        Returns a DataFrame containing all categories assigned to particular areas
        specified by their names as input, with no repetitions. In case the input
        collection of area names is empty, it is like all areas are actually specified.

        Args:
            area_names (set[str]): A collection of unique area names (e.g., 'Medicine',
                                  'Computer Science') to filter the categories by.
                                  If the set is empty, all categories from all areas/journals
                                  in the database are returned.

        Returns:
            pd.DataFrame: A DataFrame with a single column:
                          - 'category' (str): The name of the category.
        """
        conn = None
        try:
            conn = sqlite3.connect(self.getDbPathOrUrl())
            params = [] # Initialize parameters list

            # Determine the area IDs based on the provided area names
            if not area_names:
                # If no area names are provided, we query for all distinct categories.
                query = """
                    SELECT DISTINCT C.category AS category
                    FROM Category C
                """

                # Revised query for 'all areas specified' (i.e., all categories assigned to any journal)
                query = """
                    SELECT DISTINCT C.category AS category
                    FROM HasCategory HC
                    JOIN Category C ON HC.category_id = C.category_id

                """ # query to get all distinct categories assigned to any journal 
                params = []
            else:
                # Retrieve the actual area_ids from the Area table using the provided area_names
                area_name_placeholders = ','.join(['?'] * len(area_names))
                area_id_lookup_query = f"SELECT area_id FROM Area WHERE area IN ({area_name_placeholders})"  # query to get area IDs

                cursor = conn.cursor()
                cursor.execute(area_id_lookup_query, list(area_names))
                fetched_area_ids = {row[0] for row in cursor.fetchall()}  # we store fetched area IDs in a set
                cursor.close()

                if not fetched_area_ids:
                    return pd.DataFrame(columns=['category']) # if no valid area IDs are found, return with just 'category' column

                # Construct the main query using the fetched area_ids
                # We select DISTINCT category names from journals linked to these areas
                main_query_placeholders = ','.join(['?'] * len(fetched_area_ids))
                query = f"""
                    SELECT DISTINCT C.category AS category
                    FROM HasArea HA
                    JOIN HasCategory HC ON HA.journal_id = HC.journal_id
                    JOIN Category C ON HC.category_id = C.category_id
                    WHERE HA.area_id IN ({main_query_placeholders})
                """
                params = list(fetched_area_ids)

            # Execute the determined query
            df = pd.read_sql_query(query, conn, params=params)

            # The result is a DataFrame with distinct categories
            return df

        except sqlite3.Error as e:
            print(f"Database error in getCategoriesAssignedToAreas: {e}")
            return pd.DataFrame(columns=['category']) # Ensure correct column in case of error
        finally:
            if conn:
                conn.close()

    def getAreasAssignedToCategories(self, category_names: set[str]) -> pd.DataFrame:
        """
        Returns a DataFrame containing all areas assigned to journals that belong
        to the particular categories specified by their names as input, with no repetitions.
        If the input collection of category names is empty, it's like all categories
        are actually specified (i.e., all areas from all journals in the database are returned).

        Args:
            category_names (set[str]): A collection of unique category names (e.g., 'Medicine',
                                      'Computer Science') to filter the areas by.
                                      If the set is empty, all areas from all categories/journals
                                      in the database are considered.

        Returns:
            pd.DataFrame: A DataFrame with a single column:
                          - 'area' (str): The name of the area.
        """
        conn = None
        try:
            conn = sqlite3.connect(self.getDbPathOrUrl())
            params = [] # Initialize parameters list


            if not category_names:
                # If no category names are provided, query for all distinct areas linked to any journal
                query = """
                    SELECT DISTINCT A.area AS area
                    FROM HasArea HA
                    JOIN Area A ON HA.area_id = A.area_id
                """
                params = []
            else:
                # Get the actual category_ids from the Category table using the provided category_names
                category_name_placeholders = ','.join(['?'] * len(category_names))
                category_id_lookup_query = f"SELECT category_id FROM Category WHERE category IN ({category_name_placeholders})"  # query to get category IDs

                cursor = conn.cursor()
                cursor.execute(category_id_lookup_query, list(category_names))
                fetched_category_ids = {row[0] for row in cursor.fetchall()}
                cursor.close()

                if not fetched_category_ids:
                    return pd.DataFrame(columns=['area']) # Return with just 'area' column if no valid category IDs are found

                # Construct the main query using the fetched category_ids
                # Select DISTINCT area names from journals linked to these categories
                main_query_placeholders = ','.join(['?'] * len(fetched_category_ids))
                query = f"""
                    SELECT DISTINCT A.area AS area
                    FROM HasCategory HC
                    JOIN HasArea HA ON HC.journal_id = HA.journal_id
                    JOIN Area A ON HA.area_id = A.area_id
                    WHERE HC.category_id IN ({main_query_placeholders})
                """
                params = list(fetched_category_ids)

            # Execute the determined query
            df = pd.read_sql_query(query, conn, params=params)

            # The result is a DataFrame with distinct areas
            return df

        except sqlite3.Error as e:
            print(f"Database error in getAreasAssignedToCategories: {e}")
            return pd.DataFrame(columns=['area']) # Ensure correct column in case of error
        finally:
            if conn:
                conn.close()


                
    def getJournalsByArea(self, area_names: set[str]) -> pd.DataFrame:
        """
        Returns a DataFrame containing journal identifiers (ISSN/EISSN) for journals
        that are associated with the specified areas.
        
        Args:
            area_names (set[str]): Set of area names to filter by. If empty, returns all journals.
            
        Returns:
            pd.DataFrame: DataFrame with 'identifier' column containing combined ISSN/EISSN strings
        """
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            
            if not area_names:
                # If no areas specified, get all journal identifiers
                query = """
                    SELECT DISTINCT GROUP_CONCAT(JI.identifier, '; ') AS identifier
                    FROM JournalIdentifier JI
                    GROUP BY JI.journal_id
                """  # query to concatenate all identifiers for each journal
                df = pd.read_sql_query(query, conn)

            else:
                # Get journals associated with the specified areas
                placeholders = ','.join(['?'] *len(area_names))
                query = f"""
                    SELECT DISTINCT GROUP_CONCAT(JI.identifier, '; ') AS identifier
                    FROM Area A
                    JOIN HasArea HA ON A.area_id = HA.area_id
                    JOIN JournalIdentifier JI ON HA.journal_id = JI.journal_id
                    WHERE A.area IN ({placeholders})
                    GROUP BY JI.journal_id
                """
                df = pd.read_sql_query(query, conn, params=list(area_names))
                
            return df
            
        except sqlite3.Error as e:
            print(f"Database error in getJournalsByArea: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()            
    

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# SILVIA

# From csv file to Graph db


from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import read_csv


class JournalUploadHandler(UploadHandler):
    """
    A handler class for uploading journal metadata to a graph database.
    
    Inherits from UploadHandler and implements the specific logic for processing journal data 
    from CSV files and converting it into RDF triples for storage in a Blazegraph SPARQL endpoint.
    """
    def __init__(self):
        super().__init__()

   
    def pushDataToDb(self, path):
        # Check if database endpoint is configured
        db_endpoint = self.getDbPathOrUrl()
        if not db_endpoint:
            return False

        # Read csv file with pandas--> df:
        file_csv = read_csv(path, 
                          keep_default_na=False,  #empty cells --> empty strings
                          dtype={
                              "Journal title": "string",
                              "Journal ISSN (print version)": "string",
                              "Journal EISSN (online version)": "string",
                              "Languages in which the journal accepts manuscripts": "string",
                              "Publisher": "string",
                              "DOAJ Seal": "string",  # will be converted to boolean 
                              "Journal license": "string",
                              "APC": "string"         # will be converted to boolean
                          })
        
        # Initialize RDF graph
        graph = Graph()
        
        # Define RDF classes and properties
        Journal = URIRef("https://schema.org/Periodical")

        title = URIRef("https://schema.org/name")
        identifier = URIRef("https://schema.org/identifier")
        language = URIRef("https://schema.org/inLanguage")
        publisher = URIRef("https://schema.org/publisher")
        seal = URIRef("https://www.wikidata.org/wiki/Q73548471")
        license = URIRef("https://schema.org/license")
        apc = URIRef("https://www.wikidata.org/wiki/Q15291071") 

       
        base_url = "https://github.com/elenavalente31/data_flamess"

        # Iterate over each row 
        for idx, row in file_csv.iterrows():
        
            # Create local unique identifier for each journal
            local_id = "journal-" + str(idx)
            subj = URIRef(base_url+ local_id)
            
            
            graph.add((subj, RDF.type, Journal)) 
            
            # Add title 
            if row["Journal title"]:
                title_value = row["Journal title"].strip()
                graph.add((subj, title, Literal(title_value)))

            # Collect and store identifiers
            identifiers = []

            if row["Journal ISSN (print version)"]:
                issn_value = row["Journal ISSN (print version)"].strip()
                identifiers.append(issn_value)
            
            if row["Journal EISSN (online version)"]:
                eissn_value = row["Journal EISSN (online version)"].strip()
                identifiers.append(eissn_value)

            # Add identifiers
            if identifiers:
                combined_identifier = "; ".join(identifiers) # from a list of strings to a unique string
                graph.add((subj, identifier, Literal(combined_identifier.strip())))
             
            # Add languages 
            if row["Languages in which the journal accepts manuscripts"]:
                languages_str = row["Languages in which the journal accepts manuscripts"]
                languages_list = languages_str.split(",") # from a unique string to a list of strings
                for lang in languages_list:
                    clean_lang = lang.strip()
                    if clean_lang:  
                        graph.add((subj, language, Literal(clean_lang)))

            # Add publisher
            if row["Publisher"]:
                publisher_value = row["Publisher"].strip()
                graph.add((subj, publisher, Literal(publisher_value)))
            
            # Convert DOAJ Seal (Yes/No) to boolean and add to graph
            if row["DOAJ Seal"]:
                doaj_clean = row["DOAJ Seal"].strip().lower()  # .lower()= to avoid ambiguity comparing strings
                doaj_value = True if doaj_clean == "yes" else False
                graph.add((subj, seal, Literal(doaj_value)))
            
            # Add license (unique string)
            if row["Journal license"]:
                license_value = row["Journal license"].strip()
                graph.add((subj, license, Literal(license_value)))
            
            # Convert APC (Yes/No) to boolean and add to graph
            if row["APC"]:
                apc_clean = row["APC"].strip().lower()  # .lower()= to avoid ambiguity comparing strings
                apc_value = True if apc_clean == "yes" else False
                graph.add((subj, apc, Literal(apc_value)))

       
        # Connect to SPARQL endpoint and upload all triples
        store = SPARQLUpdateStore()     # proxy
        endpoint = self.getDbPathOrUrl()
        
        
        store.open((endpoint, endpoint))
        for triple in graph.triples((None, None, None)):   # we used triples method from Graph class
            store.add(triple)
        store.close()

        return True

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# ELENA

from sparql_dataframe import get

# Definition of the QueryHandler and JournalQueryHandler classes


class JournalQueryHandler(QueryHandler):
    
    PREFIXES = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        PREFIX wiki: <https://www.wikidata.org/wiki/> 
        """
    
    BASE_QUERY = """
        SELECT ?journal ?title ?identifier (GROUP_CONCAT(DISTINCT ?lang; SEPARATOR=", ") AS ?languages) ?publisher ?seal ?license ?apc
        WHERE {{
            ?journal rdf:type schema:Periodical ;
                    schema:name ?title ;
                    schema:identifier ?identifier ;
                    schema:inLanguage ?lang ;
                    wiki:Q73548471 ?seal ;
                    schema:license ?license ;
                    wiki:Q15291071 ?apc .

            OPTIONAL {{ ?journal schema:publisher ?publisher . }}

            {filter}
        }}
        GROUP BY ?journal ?title ?identifier ?publisher ?seal ?license ?apc
        """
    
    def __init__(self):
        super().__init__()



    def getById(self, id):
        
        if not id:
            return pd.DataFrame() # if there's no value specified as id, or if it's empty, returns an empty DataFrame

        filter_id = f '''
            FILTER(
                STR(?identifier) = "{id}" ||          # Single ID
                STRSTARTS(STR(?identifier), "{id}; ") ||  # First of two IDs
                STRENDS(STR(?identifier), "; {id}")       # Second of two IDs
            )
        '''
        
        query = self.PREFIXES + self.BASE_QUERY.format(filter=filter_id) # final query of the method, that contains prefixes, base query and the specific filter

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df


    def getAllJournals(self):

        # here there's no filter applied to the basic query. the method just returns all the journals as they are stored in the database

        query= self.PREFIXES + self.BASE_QUERY.format(filter="")

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)
            
        return df

    
    
    def getJournalsWithTitle(self, partialTitle):

        # filter_title is the specific filter for this method. It basically assures that the partialTitle specified will match perfectly and/or partially
        
        filter_title= f'FILTER(CONTAINS(LCASE(?title), LCASE("{partialTitle}")))' 
        
        query= self.PREFIXES + self.BASE_QUERY.format(filter= filter_title)  # the filter gets applied to the final query of the method

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

    

    def getJournalsPublishedBy(self, partialName):

        if not partialName:
            
            return pd.DataFrame() #if there is not a value in input, or if there's an empty value, returns an empty DataFrame

        # filter_publisher is the specific filter for this method. It basically assures that the partialName specified will match perfectly and/or partially
        
        filter_publisher = f'FILTER(CONTAINS(LCASE(?publisher), LCASE("{partialName}")))'

        query= self.PREFIXES + self.BASE_QUERY.format(filter=filter_publisher)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df
    
    
    def getJournalsWithLicense(self, licenses: set[str]):
        """
        Returns a DataFrame containing all journals that have any of the specified licenses.
        If the input set is empty, returns all journals.
        
        Returns:
            pd.DataFrame: A DataFrame containing matching journals
        """
        if not licenses:
            return self.getAllJournals()
    
        # Clean and prepare license strings
        cleaned_licenses = {license.strip().upper() for license in licenses if license.strip()}
    
        # Build the license filter condition
        license_conditions = []
        for license in cleaned_licenses:
            # Each license can match in 4 possible ways in the string:
            # 1. Exact match
            # 2. At start of list
            # 3. In middle of list
            # 4. At end of list
            conditions = [
                f'STR(?license) = "{license}"',
                f'STRSTARTS(STR(?license), "{license}, ")',
                f'CONTAINS(STR(?license), ", {license}, ")',
                f'STRENDS(STR(?license), ", {license}")'
            ]
            license_conditions.append(f'({" || ".join(conditions)})')
    
        # Combine all license conditions with OR
        filter_license = f'FILTER({" || ".join(license_conditions)})' if license_conditions else ""
    
        query = self.PREFIXES + self.BASE_QUERY.format(filter=filter_license)
    
        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)
    
        return df
    

    def getJournalsWithAPC(self):

        filter_tapc= f'FILTER(LCASE(STR(?apc)) = "true")'  # creates the filter for the boolean "true" 
        
        query= self.PREFIXES + self.BASE_QUERY.format(filter= filter_tapc) # applies it to the final query of the method
        
        endpoint = self.getDbPathOrUrl()
        
        df = get(endpoint, query, True)

        return df
        


    def getJournalsWithoutAPC(self): 

        filter_fapc = f'FILTER(LCASE(STR(?apc)) = "false")'  # creates the filter for the boolean "false"
       
        query= self.PREFIXES + self.BASE_QUERY.format(filter=filter_fapc)  # applies it to the final query of the method
        
        endpoint = self.getDbPathOrUrl()
        
        df = get(endpoint, query, True)

        return df
    


    def getJournalsWithDOAJSeal(self):
     
        filter_seal= f'FILTER(LCASE(STR(?seal)) = "true")' # creates the filter for the boolean value "true"
        
        query = self.PREFIXES+ self.BASE_QUERY.format(filter=filter_seal) # applies it to the final query of the method

        endpoint = self.getDbPathOrUrl()
        
        df = get(endpoint, query, True)
        return df


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

