from impl import *

import pandas as pd

class BasicQueryEngine:
    """
    BasicQueryEngine is a class that provides methods to query and manipulate journal and category data.
    It allows for adding and cleaning query handlers, retrieving journals and categories based on various criteria,
    and getting entities by their IDs.
    
    Attributes:
        journalQuery (list): A list of journal query handlers.
        categoryQuery (list): A list of category query handlers.
        
    Methods:
        cleanJournalHandlers(): Cleans the journal query handlers.
        cleanCategoryHandlers(): Cleans the category query handlers.
        addJournalHandler(handler): Adds a journal query handler to the list.
        addCategoryHandler(handler): Adds a category query handler to the list.
        getEntityById(id): Retrieves a journal or category entity by its ID.
        getAllJournals(): Retrieves all journal entities.
        getJournalsWithTitle(partialTitle): Retrieves journals with a title that contains the specified partial title.
        getJournalsPublishedBy(partialName): Retrieves journals published by a publisher with a name that contains the specified partial name.
        getJournalsWithLicense(licenses): Retrieves journals with a specific license.
        getJournalsWithAPC(): Retrieves journals with an Article Processing Charge (APC).
        getJournalsWithDOAJSeal(): Retrieves journals with a DOAJ seal.
        getAllCategories(): Retrieves all category entities.
        getAllAreas(): Retrieves all area entities.
        getCategoriesWithQuartile(quartiles): Retrieves categories with a specific quartile.
        getCategoriesAssignedToAreas(area_ids): Retrieves categories assigned to specific areas.
        getAreasAssignedToCategories(category_ids): Retrieves areas assigned to specific categories.

    """
    def __init__(self):
        self.journalQuery = []
        self.categoryQuery = []


    def cleanJournalHandlers(self):
        """
        Cleans the journal query handlers by resetting the journalQuery list.
        """
        self.journalQuery = []  # reset the list of journal query handlers removing all of them
        return True
    

    def cleanCategoryHandlers(self):
        """
        Cleans the category query handlers by resetting the categoryQuery list.
        """
        self.categoryQuery = [] # reset the list of category query handlers
        return True
    

    def addJournalHandler(self, handler: JournalQueryHandler):
        """
        Adds a journal query handler to the engine's collection of journal handlers.
        """
        if not isinstance(handler, JournalQueryHandler):  # check if the provided handler is a JournalQueryHandler instance
            return False
        self.journalQuery.append(handler)  # add the valid journal handler to the list
        return True
    
    
    def addCategoryHandler(self, handler: CategoryQueryHandler):
        """
        Adds a category query handler to the engine's collection of category handlers.
        """
        if not isinstance(handler, CategoryQueryHandler):  # check if the handler is a CategoryQueryHandler instance 
            return False
        self.categoryQuery.append(handler)  #add the category handler to the list
        return True
    

    def getCategoryById(self, id):
        """
        It returns a list "category" of Category linked to a specified Id (therefore, to a specific Journal)
        """
        if not id:
            return []

        id_list = [item.strip() for item in id.split(';')] #creates an id_list with the input id or ids

        all_dfs= []

        for handler in self.categoryQuery: #for each id in the list, it searches for a correspondence in categoryQuery, using the method getById.
            for item in id_list:
                if item:
                    df= handler.getById(item)
                    if not df.empty:           
                        all_dfs.append(df)      #in the end, the loop fills the all_dfs list with the resulting dfs from the query.
        
        if not all_dfs:
            return []
        
        merged_df = pd.concat(all_dfs).reset_index(drop=True)       #creates a merged df

        categories = []         #creates an empty list (which will be our return), and an empty set
        seen = set()

        for _, row in merged_df.iterrows():         #starts the iteration over the merged_df and retrieves category and quartile from it
            cat_list = row.get('category', [])
            quartile_list = row.get('quartile', [])

            # checks that the lists are true
            if not isinstance(cat_list, list) or not isinstance(quartile_list, list):
                continue

            for i, cat in enumerate(cat_list):
                quartile = quartile_list[i] if i < len(quartile_list) else None #checks the quartile lenght, which should be the same as the number of categories
                key = (cat, quartile)
                if key not in seen:
                    seen.add(key)
                    categories.append(Category([item], category=cat, quartile=quartile))


        return categories
        

    def getAreaById(self, id):
        """
        It returns a list of Area linked to a specified Id (therefore, to a specific Journal)
        """
        if not id:
            return []

        id_list = [item.strip() for item in id.split(';') if item.strip()]

        unique_areas = set() # creates a set to get all the unique values of areas

        for handler in self.categoryQuery:
            for item_id in id_list:
                df = handler.getById(item_id)
                if df is not None and not df.empty:
                    # gets the values of areas from the df 
                    areas_df = df.get('area', [])

                    # if areas_df is not a list, it makes it a list so that it can iterate 
                    if isinstance(areas_df, pd.Series):
                        # if areas_df is a Series, it iterates over it
                        for entry in areas_df:
                            if isinstance(entry, list):
                                for area_name in entry:
                                    if area_name and pd.notna(area_name):
                                        unique_areas.add(str(area_name).strip())
                            elif pd.notna(entry): # check if it's a single value and not null
                                unique_areas.add(str(entry).strip())
                    elif isinstance(areas_df, list):
                        # if it's already a list, it returns an empty or a pre-existing one
                        for area_name in areas_df:
                            if area_name and pd.notna(area_name):
                                unique_areas.add(str(area_name).strip())
                    elif pd.notna(areas_df): # if it's a single value (not a list, neither a Series)
                        unique_areas.add(str(areas_df).strip())

        areas = []
        for area_name in unique_areas:
            areas.append(Area([area_name])) # it passes a list to the constructor
            
        return areas

       
    def getEntityById(self, id):
        
        """" 
        This method returns entities given their IDs. The ID of an entity 'connects' the graph database and the relational database.
        Given the input id:
        a) the first for loop searches in journalQuery (using the getById method) for information about the journal; if it finds informations, 
        builds the object Journal;
        b) the second for loop searches in the categoryQuery (using the getById method) for information about the journal; if it finds some, 
        builds the obects Category and Area.

        """
        
        all_dfs = []

        # First attempt: search in journalQuery
        for handler in self.journalQuery:
            df = handler.getById(id)
            if df is not None and not df.empty:
                all_dfs.append(df.fillna(""))
        
        

        if all_dfs: #if something was found:
            
            merged_df = pd.concat(all_dfs).reset_index(drop=True)

            for col in merged_df.columns:
                if merged_df[col].apply(lambda x: isinstance(x, list)).any():
                    merged_df[col] = merged_df[col].apply(str)

            merged_df = merged_df.drop_duplicates()
            # Take only the first row to build the Journal
            row = merged_df.iloc[0]

            # Use get with default "" to avoid NoneType error
            id_str = row.get('identifier', "")
            lang_str = row.get('languages', "")
            
            journal = Journal (  
                identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings --> ["1234-6789","3456-6789"]
                title= row['title'],
                languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings
                seal=row['seal'],
                licence=row['license'],
                apc=row['apc'],
                publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # pd.notna checks if the obtained value is not NaN 
                                                                                                # If the value exists and is not NaN, assigns it to 'publisher', otherwise None. 

                categories=self.getCategoryById(id_str),
                areas=self.getAreaById(id_str)  

            )

            return journal
        
        else: 
            all_dfs = [] #if the query in journalQuery was not successful:

            for handler in self.categoryQuery: #search in categoryQuery
                df = handler.getById(id)
                if df is not None and not df.empty:
                    all_dfs.append(df.fillna(""))
            
            if not all_dfs:  
                return None

            merged_df = pd.concat(all_dfs).reset_index(drop=True)

            for col in merged_df.columns: #since there are columns that contain lists, transform the lists into strings
                if merged_df[col].apply(lambda x: isinstance(x, list)).any():
                    merged_df[col] = merged_df[col].apply(str)

            merged_df = merged_df.drop_duplicates()

            if merged_df.empty:
                return None  
            else:
                
                row = merged_df.iloc[0]  
                
                category = Category(
                    identifiers=row["identifier"],
                    category=row["category"],
                    quartile=row["quartile"]
                )

                area = Area(
                    identifiers=row["area"]
                )

                return category, area #returns the objects Category and Area
 
            

    
   

    def getAllJournals(self):
        """
        Retrieves all journal entities available through the registered journal handlers.
    
        Returns:
            list[Journal]: A list of all unique Journal entities available.
        """
        all_dfs = []
        for handler in self.journalQuery:
            df = handler.getAllJournals()
            if not df.empty:
                df = df.fillna("") #empty string
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
            allJournalsList = []
            for _, row in merged_df.iterrows():
                # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
                id_str = row.get('identifier', "")
                lang_str = row.get('languages', "")
                
                journal = Journal (  
                    identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings --> ["1234-6789","3456-6789"]
                    title= row['title'],
                    languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings
                    seal=row['seal'],
                    licence=row['license'],
                    apc=row['apc'],
                    publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # pd.notna checks if the obtained value is not NaN 
                                                                                                  # If the value exists and is not NaN, assigns it to 'publisher', otherwise None. 
        
                    categories=self.getCategoryById(id_str),
                    areas=self.getAreaById(id_str)  
                )
                allJournalsList.append(journal)

            return allJournalsList
        
    
    def getJournalsWithTitle(self, partialTitle):
        """
        Retrieves journals whose titles contain the specified partial string.
        
        Returns:
            list[Journal]: A list of Journal entities matching the title criteria.
        """
        all_dfs= []
        for handler in self.journalQuery:
            df = handler.getJournalsWithTitle(partialTitle)
            if not df.empty:
                df = df.fillna("") #empty string
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
            journalsWithTitleList = []
            for _, row in merged_df.iterrows():
                # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
                id_str = row.get('identifier', "")
                lang_str = row.get('languages', "")
                
                journal = Journal (  
                    identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
                    title= row['title'],
                    languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings
                    seal=row['seal'],
                    licence=row['license'],
                    apc=row['apc'],
                    publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
                    categories=self.getCategoryById(id_str),
                    areas=self.getAreaById(id_str)  
                )
                journalsWithTitleList.append(journal)

            return journalsWithTitleList
        

    def getJournalsPublishedBy(self, partialName):
        """
        Retrieves journals published by publishers whose names contain the specified partial string. 
    
        Returns:
            list[Journal]: A list of Journal entities matching the publisher criteria.
        """
        if not partialName:
            return []
        all_dfs = []
        for handler in self.journalQuery:
            df = handler.getJournalsPublishedBy(partialName)
            if not df.empty:
                df = df.fillna("") #empty string
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
            journalsPublishedByList = []
            for _, row in merged_df.iterrows():
                # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
                id_str = row.get('identifier', "")
                lang_str = row.get('languages', "")

                journal = Journal (  
                    identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], # creates a list of strings identifiers --> ["1234-6789","3456-6789"]
                    title= row['title'],
                    languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], # creates a list of strings 'languages'
                    seal=row['seal'],
                    licence=row['license'],
                    apc=row['apc'],
                    publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
                    categories=self.getCategoryById(id_str),
                    areas=self.getAreaById(id_str)  
                )
                journalsPublishedByList.append(journal)

            return journalsPublishedByList
        
        
    def getJournalsWithLicense(self,license):
        """
        Retrieves journals that have the specified license.
        
        Returns:
            list[Journal]: A list of Journal entities with the specified license.
        """
        all_dfs= []
        for handler in self.journalQuery:
            df = handler.getJournalsWithLicense(license)
            if not df.empty:
                df = df.fillna("") #empty string
                all_dfs.append(df)

        if not all_dfs:
            return []
        else: 
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

            journalsWithLicenseList = []
            for _, row in merged_df.iterrows():
                # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
                id_str = row.get('identifier', "")
                lang_str = row.get('languages', "")
                
                journal = Journal (  
                    identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
                    title= row['title'],
                    languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings 'languages'
                    seal=row['seal'],
                    licence=row['license'],
                    apc=row['apc'],
                    publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
                    categories=self.getCategoryById(id_str),
                    areas=self.getAreaById(id_str) 
                )
                journalsWithLicenseList.append(journal)

            return journalsWithLicenseList
    

    def getJournalsWithAPC(self):
        """
        Retrieves journals that specify an Article Processing Charge (APC).
    
        Returns:
            list[Journal]: A list of Journal entities that have APCs.
        """
        all_dfs= []
        for handler in self.journalQuery:
            df = handler.getJournalsWithAPC()
            if not df.empty:
                df = df.fillna("") #empty string
                all_dfs.append(df)

        if not all_dfs:
            return []
        else: 
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
            journalsWithAPC_list = []
            for _, row in merged_df.iterrows():
                # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
                id_str = row.get('identifier', "")
                lang_str = row.get('languages', "")
  
                journal = Journal (  
                    identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
                    title= row['title'],
                    languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings 'languages'
                    seal=row['seal'],
                    licence=row['license'],
                    apc=row['apc'],
                    publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
                    categories=self.getCategoryById(id_str),
                    areas=self.getAreaById(id_str) 
                )
                journalsWithAPC_list.append(journal)

            return journalsWithAPC_list
        

    def getJournalsWithDOAJSeal(self):
        """
        Retrieves journals that have been awarded the DOAJ Seal, which indicates they meet additional quality criteria beyond standard inclusion in DOAJ.
    
        Returns:
            list[Journal]: A list of Journal entities with the DOAJ Seal.
        """
        all_dfs= []
        for handler in self.journalQuery:
            df = handler.getJournalsWithDOAJSeal()
            if not df.empty:
                df = df.fillna("") #empty string
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
            journalsWithSealList = []
            for _, row in merged_df.iterrows():
                # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
                id_str = row.get('identifier', "")
                lang_str = row.get('languages', "")
                
                journal = Journal (  
                    identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
                    title= row['title'],
                    languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings 'languages'
                    seal=row['seal'],
                    licence=row['license'],
                    apc=row['apc'],
                    publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
                    categories=self.getCategoryById(id_str),
                    areas=self.getAreaById(id_str) 
                )
                journalsWithSealList.append(journal)

            return journalsWithSealList
        
        
    def getAllCategories(self):
        """
        Retrieves all category entities from the category query handlers.
        
        Returns:
            list[Category]: A list of all category entities
        """
        all_dfs= []
        for handler in self.categoryQuery:
            df = handler.getAllCategories()
            if not df.empty:
                df = df.fillna("")  # clean up the NaNs by replacing them
                all_dfs.append(df)

        if not all_dfs:  # if no category is found return an empty list
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
        
            allCategoriesList = []
            seen_categories = set()
            for _, row in merged_df.iterrows(): 
                category_name = row['category'] # retrieve category names
                if category_name in seen_categories: # if the category has already been encountered skip to the next
                    continue

                seen_categories.add(category_name)
                category = Category(
                    identifiers=[category_name],
                    category=category_name
                )  # construct Category object
                allCategoriesList.append(category)
            
            return allCategoriesList
    

    def getAllAreas(self):
        """
        Retrieves all area entities from the category query handlers.

        Returns:
            list[Area]: A list of all area entities.
        """
        all_dfs= []
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            if not df.empty:
                df = df.fillna("")
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

            allAreasList = []
            seen_areas = set()  # to avoid duplicates
            for _, row in merged_df.iterrows():
                area_name = row['area']
                if area_name in seen_areas: # if the area has already been encountered skip to the next
                    continue
                        
                seen_areas.add(area_name)
                area = Area(identifiers=[area_name])  # construct the Area object
                allAreasList.append(area)
                    
            return allAreasList
        
        
    def getCategoriesWithQuartile(self, quartiles):
        """
        Retrieves categories with specific quartiles
            
        Returns:
            list[Category]: List of categories with the specified quartiles
        """
        all_dfs= []
        for handler in self.categoryQuery:
            df = handler.getCategoriesWithQuartile(quartiles)
            if not df.empty:
                df = df.fillna("")
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
        
            categoriesWithQuartileList = []
            for _, row in merged_df.iterrows():
                    identifier = f"{row['category']}:{row['quartile']}"
                    category = Category(
                        identifiers=[identifier],
                        category=row['category'],
                        quartile=row['quartile']
                    )
                    categoriesWithQuartileList.append(category)

            return categoriesWithQuartileList
        

    def getCategoriesAssignedToAreas(self, area_names: set[str]):
        """
        Retrieves all categories assigned to particular areas specified by their names.

        Returns:
            list[Category]: A list of unique Category entities assigned to the specified areas.
        """
        all_dfs= []
        for handler in self.categoryQuery:
            df = handler.getCategoriesAssignedToAreas(area_names)
            if not df.empty:
                df = df.fillna("")
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

            categoriesAssignedToAreasList = []
            seen_category_identifiers = set() 
            for _, row in merged_df.iterrows():
                category_name = row['category']
                category_quartile = row['quartile'] if 'quartile' in row else None
                category_id = row['category_id'] if 'category_id' in row else None

                if category_id:
                    identifier = category_id
                elif category_name and category_quartile:
                    identifier = f"{category_name}:{category_quartile}"
                else: 
                    identifier = category_name

                if identifier in seen_category_identifiers:
                    continue
                    
                seen_category_identifiers.add(identifier)
                    
                category = Category(
                    identifiers=[identifier],
                    category=category_name,
                    quartile=category_quartile
                )
                categoriesAssignedToAreasList.append(category)
                    
            return categoriesAssignedToAreasList


    def getAreasAssignedToCategories(self, category_names: set[str]):
        """
        Retrieves all areas assigned to journals that belong to particular categories.

        Returns:
            list[Area]: A list of unique Area entities assigned to the specified categories.
        """
        all_dfs= []
        for handler in self.categoryQuery:
            df = handler.getAreasAssignedToCategories(category_names)
            if not df.empty:
                df = df.fillna("")
                all_dfs.append(df)

        if not all_dfs:
            return []
        else:
            merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

            areasAssignedToCategories = []
            seen_area_identifiers = set() 
            for _, row in merged_df.iterrows():
                area_name = row['area']
                area_id = row['area_id'] if 'area_id' in row else None
                identifier = area_id if area_id else area_name

                if identifier in seen_area_identifiers:
                    continue
                    
                seen_area_identifiers.add(identifier)
                    
                area = Area(
                identifiers=[identifier]
                )
                areasAssignedToCategories.append(area)
                    
            return areasAssignedToCategories    
    


            
class FullQueryEngine(BasicQueryEngine):
    """
    FullQueryEngine is a subclass of BasicQueryEngine that provides additional methods to query and manipulate journal and category data.

    Attributes:
        journalQuery (list): A list of journal query handlers.
        categoryQuery (list): A list of category query handlers.

    Methods:
        __init__(journalQuery=None, categoryQuery=None): Initializes the FullQueryEngine with optional journal and category query handlers.
        getJournalsInCategoriesWithQuartile(category_ids, quartiles): Retrieves journals in specific categories with quartiles.
        getJournalsInAreasWithLicense(areas_ids, licenses): Retrieves journals in specific areas with licenses.
        getDiamondJournalsInAreasAndCategoriesWithQuartile(areas_ids, category_ids, quartiles): Retrieves diamond journals in areas and categories with quartiles.
    """
    def __init__(self):
        super().__init__()

    def getJournalsInCategoriesWithQuartile(self, categories: set[str], quartiles: set[str]) -> list[Journal]:
        """
        Returns journals in DOAJ with specified categories and quartiles
        
        Args:
            categories: Set of category names (empty = all categories)
            quartiles: Set of quartiles (empty = all quartiles)
            
        Returns:
            list[Journal]: Matching journals from DOAJ
        """
        # Get all DOAJ journals
        doaj_journals = self.getAllJournals()
        
        # Filter journals that have at least one matching category with the specified quartile
        result = []
        for journal in doaj_journals:
            journal_categories = journal.getCategories()
            
            # Check if the journal has at least one category that meets the criteria
            has_matching_category = False
            for cat in journal_categories:
                cat_name = cat.category
                cat_quartile = cat.quartile
                
                # Check if the category and quartile match the filters
                category_matches = (not categories) or (cat_name in categories)
                quartile_matches = (not quartiles) or (cat_quartile in quartiles)
                
                if category_matches and quartile_matches:
                    has_matching_category = True
                    break  # One matching category is enough
            
            if has_matching_category:
                result.append(journal)
        
        return result
    


    def getJournalsInAreasWithLicense(self, areas_ids: set[str], licenses: set[str]) -> list[Journal]:
        """
        Returns a list of Journal objects that:
        -Belong to at least one of the specified areas (or all if areas_ids is empty)
        - Have at least one of the specified licenses (or all if licenses is empty)

        Args:
            areas_ids (set[str]): Set of area names to filter by
            licenses (set[str]): Set of license strings to filter by

        Returns:
            list[Journal]: List of matching Journal objects
        """

        # Collects all journal identifiers for the specified areas (areas_ids)
        area_identifiers = set()

        for handler in self.categoryQuery:
            df = handler.getJournalsByArea(areas_ids)
            if not df.empty:
                for id_string in df['identifier']:
                    ids = {id.strip() for id in id_string.split(';')}  # Split the string --> list --> set
                area_identifiers.update(ids)  # update the set with another set

        # Retrieve journals with the specified licenses
        all_license_dfs = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithLicense(licenses) if licenses else handler.getAllJournals() # otherwise no license filtering
            if not df.empty:
                all_license_dfs.append(df)

        if not all_license_dfs:
            return []

        merged_license_df = pd.concat(all_license_dfs).drop_duplicates().reset_index(drop=True)

        # Filter by area only if areas are specified
        if areas_ids and area_identifiers:
            filtered_df = merged_license_df[
                merged_license_df['identifier'].apply(self.rowHasMatchingIdentifier, args=(area_identifiers,)) # The comma creates a single-item tuple for 'args', which expects a tuple
            ]
        else:
            filtered_df = merged_license_df # otherwise no area filtering


        # We apply the rowHasMatchingIdentifier function to each row's 'identifier' string.
        # The function returns True if at least one of the identifiers in the string matches an identifier from the valid_identifiers set.
        # The function returns True or False for each row, producing a boolean Series. We keep only the rows where the boolean value is True.


        # Build the Journal objects from the filtered DataFrame
        journals_list = []
        for _, row in filtered_df.iterrows():
            if not row.get('identifier'):
                continue

            categories = self.getCategoryById(row['identifier'])
            areas = self.getAreaById(row['identifier'])

            journal = Journal(
                identifiers=[id.strip() for id in row['identifier'].split(';')],
                title=row['title'].strip(),
                languages=[lang.strip() for lang in row.get('languages', "").split(',')] if row.get('languages') else [],
                seal=bool(row['seal']),
                licence=row['license'].strip(),
                apc=bool(row['apc']),
                publisher=row['publisher'].strip() if pd.notna(row['publisher']) else None,
                categories=categories,
                areas=areas
            )
            journals_list.append(journal)

        return journals_list


    def rowHasMatchingIdentifier(self, row_identifiers: str, valid_identifiers: set[str]):
        """
        Checks if at least one of the identifiers in a row matches one of the valid identifiers.

        Args:
            row_identifiers (str): String containing identifiers separated by ';'
            valid_identifiers (set[str]): Set of valid identifiers to check against

        Returns:
            bool: True if at least one identifier matches, False otherwise
        """
        if not row_identifiers:
            return False

        for identifier in row_identifiers.split(';'):
            if identifier.strip() in valid_identifiers:
                return True
        return False



    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas: set[str], categories: set[str], quartiles: set[str]) -> list[Journal]:
        """
        Returns diamond journals (no APC) that satisfy:
        - Has at least one area in the areas set
        - Has at least one category in the categories set with a quartile in the quartiles set
        
        Args:
            areas: Set of area names (empty = all areas)
            categories: Set of category names (empty = all categories)
            quartiles: Set of quartiles (empty = all quartiles)
            
        Returns:
            list[Journal]: Matching diamond journals
        """
        # Get all journals
        all_journals = self.getAllJournals()
        
        # Filter diamond journals: no APC and with DOAJ Seal
        diamond_journals = [
            j for j in all_journals 
            if not j.hasAPC() and j.hasDOAJSeal()  # keep only those ones that don't have an APC and have the DOAJ Seal (diamond open access)
        ]
        
        # Filter by areas and categories with associated quartiles
        result = []
        seen = set()
        
        for journal in diamond_journals:
            # Check areas (if not empty)
            if areas:
                journal_areas = {area.getIds()[0] for area in journal.getAreas()} # get area IDs for the journal
                if not journal_areas.intersection(areas): # if no journal's area matches the filter areas skip it
                    continue
            
            # Check categories and quartiles 
            if categories or quartiles:
                category_quartile_match = False
                
                # Verify each category of the journal
                for cat in journal.getCategories():
                    # If no specific categories are provided, consider any category
                    category_ok = not categories or cat.category in categories
                    
                    # If no specific quartiles are provided, consider any quartile
                    quartile_ok = not quartiles or cat.quartile in quartiles
                    
                    # If both conditions are met for this category
                    if category_ok and quartile_ok:
                        category_quartile_match = True
                        break
                
                if not category_quartile_match: # after checking all categories, if none matches the category/quartile filter, the journal is skipped
                    continue
            
            # Avoid duplicates
            journal_key = tuple(sorted(journal.getIds()))
            if journal_key not in seen:
                seen.add(journal_key)
                result.append(journal)
        
        return result
    






















# from impl import *

# import pandas as pd

# class BasicQueryEngine:
#     """
#     BasicQueryEngine is a class that provides methods to query and manipulate journal and category data.
#     It allows for adding and cleaning query handlers, retrieving journals and categories based on various criteria,
#     and getting entities by their IDs.
    
#     Attributes:
#         journalQuery (list): A list of journal query handlers.
#         categoryQuery (list): A list of category query handlers.
        
#     Methods:
#         cleanJournalHandlers(): Cleans the journal query handlers.
#         cleanCategoryHandlers(): Cleans the category query handlers.
#         addJournalHandler(handler): Adds a journal query handler to the list.
#         addCategoryHandler(handler): Adds a category query handler to the list.
#         getEntityById(id): Retrieves a journal or category entity by its ID.
#         getAllJournals(): Retrieves all journal entities.
#         getJournalsWithTitle(partialTitle): Retrieves journals with a title that contains the specified partial title.
#         getJournalsPublishedBy(partialName): Retrieves journals published by a publisher with a name that contains the specified partial name.
#         getJournalsWithLicense(licenses): Retrieves journals with a specific license.
#         getJournalsWithAPC(): Retrieves journals with an Article Processing Charge (APC).
#         getJournalsWithDOAJSeal(): Retrieves journals with a DOAJ seal.
#         getAllCategories(): Retrieves all category entities.
#         getAllAreas(): Retrieves all area entities.
#         getCategoriesWithQuartile(quartiles): Retrieves categories with a specific quartile.
#         getCategoriesAssignedToAreas(area_ids): Retrieves categories assigned to specific areas.
#         getAreasAssignedToCategories(category_ids): Retrieves areas assigned to specific categories.

#     """
#     def __init__(self):
#         self.journalQuery = []
#         self.categoryQuery = []


#     def cleanJournalHandlers(self):
#         """
#         Cleans the journal query handlers by resetting the journalQuery list.
#         """
#         self.journalQuery = []  # reset the list of journal query handlers removing all of them
#         return True
    

#     def cleanCategoryHandlers(self):
#         """
#         Cleans the category query handlers by resetting the categoryQuery list.
#         """
#         self.categoryQuery = [] # reset the list of category query handlers
#         return True
    

#     def addJournalHandler(self, handler: JournalQueryHandler):
#         """
#         Adds a journal query handler to the engine's collection of journal handlers.
#         """
#         if not isinstance(handler, JournalQueryHandler):  # check if the provided handler is a JournalQueryHandler instance
#             return False
#         self.journalQuery.append(handler)  # add the valid journal handler to the list
#         return True
    
    
#     def addCategoryHandler(self, handler: CategoryQueryHandler):
#         """
#         Adds a category query handler to the engine's collection of category handlers.
#         """
#         if not isinstance(handler, CategoryQueryHandler):  # check if the handler is a CategoryQueryHandler instance 
#             return False
#         self.categoryQuery.append(handler)  #add the category handler to the list
#         return True
    

#     def getCategoryById(self, id):
#         """
#         It returns a list "category" of Category linked to a specified Id (therefore, to a specific Journal)
#         """
#         if not id:
#             return []

#         id_list = [item.strip() for item in id.split(';')] #creates an id_list with the input id or ids

#         all_dfs= []

#         for handler in self.categoryQuery: #for each id in the list, it searches for a correspondence in categoryQuery, using the method getById.
#             for item in id_list:
#                 if item:
#                     df= handler.getById(item)
#                     if not df.empty:           
#                         all_dfs.append(df)      #in the end, the loop fills the all_dfs list with the resulting dfs from the query.
        
#         if not all_dfs:
#             return []
        
#         merged_df = pd.concat(all_dfs).reset_index(drop=True)       #creates a merged df

#         categories = []         #creates an empty list (which will be our return), and an empty set
#         seen = set()

#         for _, row in merged_df.iterrows():         #starts the iteration over the merged_df and retrieves category and quartile from it
#             cat_list = row.get('category', [])
#             quartile_list = row.get('quartile', [])

#             # checks that the lists are true
#             if not isinstance(cat_list, list) or not isinstance(quartile_list, list):
#                 continue

#             for i, cat in enumerate(cat_list):
#                 quartile = quartile_list[i] if i < len(quartile_list) else None #checks the quartile lenght, which should be the same as the number of categories
#                 key = (cat, quartile)
#                 if key not in seen:
#                     seen.add(key)
#                     categories.append(Category([item], category=cat, quartile=quartile))


#         return categories
        

#     def getAreaById(self, id):
#         """
#         It returns a list of Area linked to a specified Id (therefore, to a specific Journal)
#         """
#         if not id:
#             return []

#         id_list = [item.strip() for item in id.split(';') if item.strip()]

#         unique_areas = set() # creates a set to get all the unique values of areas

#         for handler in self.categoryQuery:
#             for item_id in id_list:
#                 df = handler.getById(item_id)
#                 if df is not None and not df.empty:
#                     # gets the values of areas from the df 
#                     areas_df = df.get('area', [])

#                     # if areas_df is not a list, it makes it a list so that it can iterate 
#                     if isinstance(areas_df, pd.Series):
#                         # if areas_df is a Series, it iterates over it
#                         for entry in areas_df:
#                             if isinstance(entry, list):
#                                 for area_name in entry:
#                                     if area_name and pd.notna(area_name):
#                                         unique_areas.add(str(area_name).strip())
#                             elif pd.notna(entry): # check if it's a single value and not null
#                                 unique_areas.add(str(entry).strip())
#                     elif isinstance(areas_df, list):
#                         # if it's already a list, it returns an empty or a pre-existing one
#                         for area_name in areas_df:
#                             if area_name and pd.notna(area_name):
#                                 unique_areas.add(str(area_name).strip())
#                     elif pd.notna(areas_df): # if it's a single value (not a list, neither a Series)
#                         unique_areas.add(str(areas_df).strip())

#         areas = []
#         for area_name in unique_areas:
#             areas.append(Area([area_name])) # it passes a list to the constructor
            
#         return areas

       
#     def getEntityById(self, id):

#         all_dfs = []

#         # First attempt: search in journalQuery
#         for handler in self.journalQuery:
#             df = handler.getById(id)
#             if df is not None and not df.empty:
#                 all_dfs.append(df.fillna(""))

#         # If nothing was found in journalQuery, search in categoryQuery
#         if not all_dfs:
#             for handler in self.categoryQuery:
#                 df = handler.getById(id)
#                 if df is not None and not df.empty:
#                     all_dfs.append(df.fillna(""))

#         # If still nothing, return None
#         if not all_dfs:
#             return None

#         # Merge found dataframes and remove duplicates
#         merged_df = pd.concat(all_dfs).reset_index(drop=True)

#         # Convert to string only columns containing lists, to avoid errors with drop_duplicates
#         for col in merged_df.columns:
#             if any(isinstance(x, list) for x in merged_df[col]):
#                 merged_df[col] = merged_df[col].astype(str)

#                 # Remove possible duplicates
#         merged_df = merged_df.drop_duplicates()         

#         if merged_df.empty:
#             # No data after concatenation (e.g. all dataframes empty)
#             return None

#         # Take only the first row to build the Journal
#         row = merged_df.iloc[0]

#         # Use get with default "" to avoid NoneType error
#         id_str = row.get('identifier', "")
#         languages_str = row.get('languages', "")


#         identifiers = id_str.split(';') if id_str else []
#         languages = languages_str.split(',') if languages_str else []

        
        
#         journal = Journal (  
#             identifiers=identifiers,
#             title= row.get('title'),
#             languages=languages,
#             seal=row.get('seal'),
#             licence=row.get('license'),
#             apc=row.get('apc'),
#             publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None,
 
#             categories=self.getCategoryById(id_str),
#             areas=self.getAreaById(id_str)  

#         )

#         return journal


#     def getAllJournals(self):
#         """
#         Retrieves all journal entities available through the registered journal handlers.
    
#         Returns:
#             list[Journal]: A list of all unique Journal entities available.
#         """
#         all_dfs = []
#         for handler in self.journalQuery:
#             df = handler.getAllJournals()
#             if not df.empty:
#                 df = df.fillna("") #empty string
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
#             allJournalsList = []
#             for _, row in merged_df.iterrows():
#                 # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
#                 id_str = row.get('identifier', "")
#                 lang_str = row.get('languages', "")
                
#                 journal = Journal (  
#                     identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings --> ["1234-6789","3456-6789"]
#                     title= row['title'],
#                     languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings
#                     seal=row['seal'],
#                     licence=row['license'],
#                     apc=row['apc'],
#                     publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # pd.notna checks if the obtained value is not NaN 
#                                                                                                   # If the value exists and is not NaN, assigns it to 'publisher', otherwise None. 
        
#                     categories=self.getCategoryById(id_str),
#                     areas=self.getAreaById(id_str)  
#                 )
#                 allJournalsList.append(journal)

#             return allJournalsList
        
    
#     def getJournalsWithTitle(self, partialTitle):
#         """
#         Retrieves journals whose titles contain the specified partial string.
        
#         Returns:
#             list[Journal]: A list of Journal entities matching the title criteria.
#         """
#         all_dfs= []
#         for handler in self.journalQuery:
#             df = handler.getJournalsWithTitle(partialTitle)
#             if not df.empty:
#                 df = df.fillna("") #empty string
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
#             journalsWithTitleList = []
#             for _, row in merged_df.iterrows():
#                 # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
#                 id_str = row.get('identifier', "")
#                 lang_str = row.get('languages', "")
                
#                 journal = Journal (  
#                     identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
#                     title= row['title'],
#                     languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings
#                     seal=row['seal'],
#                     licence=row['license'],
#                     apc=row['apc'],
#                     publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
#                     categories=self.getCategoryById(id_str),
#                     areas=self.getAreaById(id_str)  
#                 )
#                 journalsWithTitleList.append(journal)

#             return journalsWithTitleList
        

#     def getJournalsPublishedBy(self, partialName):
#         """
#         Retrieves journals published by publishers whose names contain the specified partial string. 
    
#         Returns:
#             list[Journal]: A list of Journal entities matching the publisher criteria.
#         """
#         if not partialName:
#             return []
#         all_dfs = []
#         for handler in self.journalQuery:
#             df = handler.getJournalsPublishedBy(partialName)
#             if not df.empty:
#                 df = df.fillna("") #empty string
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
#             journalsPublishedByList = []
#             for _, row in merged_df.iterrows():
#                 # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
#                 id_str = row.get('identifier', "")
#                 lang_str = row.get('languages', "")

#                 journal = Journal (  
#                     identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], # creates a list of strings identifiers --> ["1234-6789","3456-6789"]
#                     title= row['title'],
#                     languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], # creates a list of strings 'languages'
#                     seal=row['seal'],
#                     licence=row['license'],
#                     apc=row['apc'],
#                     publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
#                     categories=self.getCategoryById(id_str),
#                     areas=self.getAreaById(id_str)  
#                 )
#                 journalsPublishedByList.append(journal)

#             return journalsPublishedByList
        
        
#     def getJournalsWithLicense(self,license):
#         """
#         Retrieves journals that have the specified license.
        
#         Returns:
#             list[Journal]: A list of Journal entities with the specified license.
#         """
#         all_dfs= []
#         for handler in self.journalQuery:
#             df = handler.getJournalsWithLicense(license)
#             if not df.empty:
#                 df = df.fillna("") #empty string
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else: 
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

#             journalsWithLicenseList = []
#             for _, row in merged_df.iterrows():
#                 # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
#                 id_str = row.get('identifier', "")
#                 lang_str = row.get('languages', "")
                
#                 journal = Journal (  
#                     identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
#                     title= row['title'],
#                     languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings 'languages'
#                     seal=row['seal'],
#                     licence=row['license'],
#                     apc=row['apc'],
#                     publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
#                     categories=self.getCategoryById(id_str),
#                     areas=self.getAreaById(id_str) 
#                 )
#                 journalsWithLicenseList.append(journal)

#             return journalsWithLicenseList
    

#     def getJournalsWithAPC(self):
#         """
#         Retrieves journals that specify an Article Processing Charge (APC).
    
#         Returns:
#             list[Journal]: A list of Journal entities that have APCs.
#         """
#         all_dfs= []
#         for handler in self.journalQuery:
#             df = handler.getJournalsWithAPC()
#             if not df.empty:
#                 df = df.fillna("") #empty string
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else: 
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
#             journalsWithAPC_list = []
#             for _, row in merged_df.iterrows():
#                 # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
#                 id_str = row.get('identifier', "")
#                 lang_str = row.get('languages', "")
  
#                 journal = Journal (  
#                     identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
#                     title= row['title'],
#                     languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings 'languages'
#                     seal=row['seal'],
#                     licence=row['license'],
#                     apc=row['apc'],
#                     publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
#                     categories=self.getCategoryById(id_str),
#                     areas=self.getAreaById(id_str) 
#                 )
#                 journalsWithAPC_list.append(journal)

#             return journalsWithAPC_list
        

#     def getJournalsWithDOAJSeal(self):
#         """
#         Retrieves journals that have been awarded the DOAJ Seal, which indicates they meet additional quality criteria beyond standard inclusion in DOAJ.
    
#         Returns:
#             list[Journal]: A list of Journal entities with the DOAJ Seal.
#         """
#         all_dfs= []
#         for handler in self.journalQuery:
#             df = handler.getJournalsWithDOAJSeal()
#             if not df.empty:
#                 df = df.fillna("") #empty string
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
            
#             journalsWithSealList = []
#             for _, row in merged_df.iterrows():
#                 # Extract 'identifier' and 'languages' str values from DataFrame row, defaulting to empty string if not found
#                 id_str = row.get('identifier', "")
#                 lang_str = row.get('languages', "")
                
#                 journal = Journal (  
#                     identifiers=[id.strip() for id in id_str.split(';')] if id_str else [], #creates a list of strings identifiers --> ["1234-6789","3456-6789"]
#                     title= row['title'],
#                     languages=[lang.strip() for lang in lang_str.split(',')] if lang_str else [], #creates a list of strings 'languages'
#                     seal=row['seal'],
#                     licence=row['license'],
#                     apc=row['apc'],
#                     publisher = row.get("publisher") if pd.notna(row.get("publisher")) else None, # If the value exists and is not NaN, assigns it to 'publisher', otherwise None.
        
#                     categories=self.getCategoryById(id_str),
#                     areas=self.getAreaById(id_str) 
#                 )
#                 journalsWithSealList.append(journal)

#             return journalsWithSealList
        
        
#     def getAllCategories(self):
#         """
#         Retrieves all category entities from the category query handlers.
        
#         Returns:
#             list[Category]: A list of all category entities
#         """
#         all_dfs= []
#         for handler in self.categoryQuery:
#             df = handler.getAllCategories()
#             if not df.empty:
#                 df = df.fillna("")  # clean up the NaNs by replacing them
#                 all_dfs.append(df)

#         if not all_dfs:  # if no category is found return an empty list
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
        
#             allCategoriesList = []
#             seen_categories = set()
#             for _, row in merged_df.iterrows(): 
#                 category_name = row['category'] # retrieve category names
#                 if category_name in seen_categories: # if the category has already been encountered skip to the next
#                     continue

#                 seen_categories.add(category_name)
#                 category = Category(
#                     identifiers=[category_name],
#                     category=category_name
#                 )  # construct Category object
#                 allCategoriesList.append(category)
            
#             return allCategoriesList
    

#     def getAllAreas(self):
#         """
#         Retrieves all area entities from the category query handlers.

#         Returns:
#             list[Area]: A list of all area entities.
#         """
#         all_dfs= []
#         for handler in self.categoryQuery:
#             df = handler.getAllAreas()
#             if not df.empty:
#                 df = df.fillna("")
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

#             allAreasList = []
#             seen_areas = set()  # to avoid duplicates
#             for _, row in merged_df.iterrows():
#                 area_name = row['area']
#                 if area_name in seen_areas: # if the area has already been encountered skip to the next
#                     continue
                        
#                 seen_areas.add(area_name)
#                 area = Area(identifiers=[area_name])  # construct the Area object
#                 allAreasList.append(area)
                    
#             return allAreasList
        
        
#     def getCategoriesWithQuartile(self, quartiles):
#         """
#         Retrieves categories with specific quartiles
            
#         Returns:
#             list[Category]: List of categories with the specified quartiles
#         """
#         all_dfs= []
#         for handler in self.categoryQuery:
#             df = handler.getCategoriesWithQuartile(quartiles)
#             if not df.empty:
#                 df = df.fillna("")
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()
        
#             categoriesWithQuartileList = []
#             for _, row in merged_df.iterrows():
#                     identifier = f"{row['category']}:{row['quartile']}"
#                     category = Category(
#                         identifiers=[identifier],
#                         category=row['category'],
#                         quartile=row['quartile']
#                     )
#                     categoriesWithQuartileList.append(category)

#             return categoriesWithQuartileList
        

#     def getCategoriesAssignedToAreas(self, area_names: set[str]):
#         """
#         Retrieves all categories assigned to particular areas specified by their names.

#         Returns:
#             list[Category]: A list of unique Category entities assigned to the specified areas.
#         """
#         all_dfs= []
#         for handler in self.categoryQuery:
#             df = handler.getCategoriesAssignedToAreas(area_names)
#             if not df.empty:
#                 df = df.fillna("")
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

#             categoriesAssignedToAreasList = []
#             seen_category_identifiers = set() 
#             for _, row in merged_df.iterrows():
#                 category_name = row['category']
#                 category_quartile = row['quartile'] if 'quartile' in row else None
#                 category_id = row['category_id'] if 'category_id' in row else None

#                 if category_id:
#                     identifier = category_id
#                 elif category_name and category_quartile:
#                     identifier = f"{category_name}:{category_quartile}"
#                 else: 
#                     identifier = category_name

#                 if identifier in seen_category_identifiers:
#                     continue
                    
#                 seen_category_identifiers.add(identifier)
                    
#                 category = Category(
#                     identifiers=[identifier],
#                     category=category_name,
#                     quartile=category_quartile
#                 )
#                 categoriesAssignedToAreasList.append(category)
                    
#             return categoriesAssignedToAreasList


#     def getAreasAssignedToCategories(self, category_names: set[str]):
#         """
#         Retrieves all areas assigned to journals that belong to particular categories.

#         Returns:
#             list[Area]: A list of unique Area entities assigned to the specified categories.
#         """
#         all_dfs= []
#         for handler in self.categoryQuery:
#             df = handler.getAreasAssignedToCategories(category_names)
#             if not df.empty:
#                 df = df.fillna("")
#                 all_dfs.append(df)

#         if not all_dfs:
#             return []
#         else:
#             merged_df = pd.concat(all_dfs).reset_index(drop=True).drop_duplicates()

#             areasAssignedToCategories = []
#             seen_area_identifiers = set() 
#             for _, row in merged_df.iterrows():
#                 area_name = row['area']
#                 area_id = row['area_id'] if 'area_id' in row else None
#                 identifier = area_id if area_id else area_name

#                 if identifier in seen_area_identifiers:
#                     continue
                    
#                 seen_area_identifiers.add(identifier)
                    
#                 area = Area(
#                 identifiers=[identifier]
#                 )
#                 areasAssignedToCategories.append(area)
                    
#             return areasAssignedToCategories    
    


            
# class FullQueryEngine(BasicQueryEngine):
#     """
#     FullQueryEngine is a subclass of BasicQueryEngine that provides additional methods to query and manipulate journal and category data.

#     Attributes:
#         journalQuery (list): A list of journal query handlers.
#         categoryQuery (list): A list of category query handlers.

#     Methods:
#         __init__(journalQuery=None, categoryQuery=None): Initializes the FullQueryEngine with optional journal and category query handlers.
#         getJournalsInCategoriesWithQuartile(category_ids, quartiles): Retrieves journals in specific categories with quartiles.
#         getJournalsInAreasWithLicense(areas_ids, licenses): Retrieves journals in specific areas with licenses.
#         getDiamondJournalsInAreasAndCategoriesWithQuartile(areas_ids, category_ids, quartiles): Retrieves diamond journals in areas and categories with quartiles.
#     """
#     def __init__(self):
#         super().__init__()

#     def getJournalsInCategoriesWithQuartile(self, categories: set[str], quartiles: set[str]) -> list[Journal]:
#         """
#         Returns journals in DOAJ with specified categories and quartiles
        
#         Args:
#             categories: Set of category names (empty = all categories)
#             quartiles: Set of quartiles (empty = all quartiles)
            
#         Returns:
#             list[Journal]: Matching journals from DOAJ
#         """
#         # Get all DOAJ journals
#         doaj_journals = self.getAllJournals()
        
#         # Filter journals that have at least one matching category with the specified quartile
#         result = []
#         for journal in doaj_journals:
#             journal_categories = journal.getCategories()
            
#             # Check if the journal has at least one category that meets the criteria
#             has_matching_category = False
#             for cat in journal_categories:
#                 cat_name = cat.category
#                 cat_quartile = cat.quartile
                
#                 # Check if the category and quartile match the filters
#                 category_matches = (not categories) or (cat_name in categories)
#                 quartile_matches = (not quartiles) or (cat_quartile in quartiles)
                
#                 if category_matches and quartile_matches:
#                     has_matching_category = True
#                     break  # One matching category is enough
            
#             if has_matching_category:
#                 result.append(journal)
        
#         return result
    


#     def getJournalsInAreasWithLicense(self, areas_ids: set[str], licenses: set[str]) -> list[Journal]:
#         """
#         Returns a list of Journal objects that:
#         -Belong to at least one of the specified areas (or all if areas_ids is empty)
#         - Have at least one of the specified licenses (or all if licenses is empty)

#         Args:
#             areas_ids (set[str]): Set of area names to filter by
#             licenses (set[str]): Set of license strings to filter by

#         Returns:
#             list[Journal]: List of matching Journal objects
#         """

#         # Collects all journal identifiers for the specified areas (areas_ids)
#         area_identifiers = set()

#         for handler in self.categoryQuery:
#             df = handler.getJournalsByArea(areas_ids)
#             if not df.empty:
#                 for id_string in df['identifier']:
#                     ids = {id.strip() for id in id_string.split(';')}  # Split the string --> list --> set
#                 area_identifiers.update(ids)  # update the set with another set

#         # Retrieve journals with the specified licenses
#         all_license_dfs = []
#         for handler in self.journalQuery:
#             df = handler.getJournalsWithLicense(licenses) if licenses else handler.getAllJournals() # otherwise no license filtering
#             if not df.empty:
#                 all_license_dfs.append(df)

#         if not all_license_dfs:
#             return []

#         merged_license_df = pd.concat(all_license_dfs).drop_duplicates().reset_index(drop=True)

#         # Filter by area only if areas are specified
#         if areas_ids and area_identifiers:
#             filtered_df = merged_license_df[
#                 merged_license_df['identifier'].apply(self.rowHasMatchingIdentifier, args=(area_identifiers,)) # The comma creates a single-item tuple for 'args', which expects a tuple
#             ]
#         else:
#             filtered_df = merged_license_df # otherwise no area filtering


#         # We apply the rowHasMatchingIdentifier function to each row's 'identifier' string.
#         # The function returns True if at least one of the identifiers in the string matches an identifier from the valid_identifiers set.
#         # The function returns True or False for each row, producing a boolean Series. We keep only the rows where the boolean value is True.


#         # Build the Journal objects from the filtered DataFrame
#         journals_list = []
#         for _, row in filtered_df.iterrows():
#             if not row.get('identifier'):
#                 continue

#             categories = self.getCategoryById(row['identifier'])
#             areas = self.getAreaById(row['identifier'])

#             journal = Journal(
#                 identifiers=[id.strip() for id in row['identifier'].split(';')],
#                 title=row['title'].strip(),
#                 languages=[lang.strip() for lang in row.get('languages', "").split(',')] if row.get('languages') else [],
#                 seal=bool(row['seal']),
#                 licence=row['license'].strip(),
#                 apc=bool(row['apc']),
#                 publisher=row['publisher'].strip() if pd.notna(row['publisher']) else None,
#                 categories=categories,
#                 areas=areas
#             )
#             journals_list.append(journal)

#         return journals_list


#     def rowHasMatchingIdentifier(self, row_identifiers: str, valid_identifiers: set[str]):
#         """
#         Checks if at least one of the identifiers in a row matches one of the valid identifiers.

#         Args:
#             row_identifiers (str): String containing identifiers separated by ';'
#             valid_identifiers (set[str]): Set of valid identifiers to check against

#         Returns:
#             bool: True if at least one identifier matches, False otherwise
#         """
#         if not row_identifiers:
#             return False

#         for identifier in row_identifiers.split(';'):
#             if identifier.strip() in valid_identifiers:
#                 return True
#         return False



#     def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas: set[str], categories: set[str], quartiles: set[str]) -> list[Journal]:
#         """
#         Returns diamond journals (no APC) that satisfy:
#         - Has at least one area in the areas set
#         - Has at least one category in the categories set with a quartile in the quartiles set
        
#         Args:
#             areas: Set of area names (empty = all areas)
#             categories: Set of category names (empty = all categories)
#             quartiles: Set of quartiles (empty = all quartiles)
            
#         Returns:
#             list[Journal]: Matching diamond journals
#         """
#         # Get all journals
#         all_journals = self.getAllJournals()
        
#         # Filter diamond journals: no APC and with DOAJ Seal
#         diamond_journals = [
#             j for j in all_journals 
#             if not j.hasAPC() and j.hasDOAJSeal()  # keep only those ones that don't have an APC and have the DOAJ Seal (diamond open access)
#         ]
        
#         # Filter by areas and categories with associated quartiles
#         result = []
#         seen = set()
        
#         for journal in diamond_journals:
#             # Check areas (if not empty)
#             if areas:
#                 journal_areas = {area.getIds()[0] for area in journal.getAreas()} # get area IDs for the journal
#                 if not journal_areas.intersection(areas): # if no journal's area matches the filter areas skip it
#                     continue
            
#             # Check categories and quartiles 
#             if categories or quartiles:
#                 category_quartile_match = False
                
#                 # Verify each category of the journal
#                 for cat in journal.getCategories():
#                     # If no specific categories are provided, consider any category
#                     category_ok = not categories or cat.category in categories
                    
#                     # If no specific quartiles are provided, consider any quartile
#                     quartile_ok = not quartiles or cat.quartile in quartiles
                    
#                     # If both conditions are met for this category
#                     if category_ok and quartile_ok:
#                         category_quartile_match = True
#                         break
                
#                 if not category_quartile_match: # after checking all categories, if none matches the category/quartile filter, the journal is skipped
#                     continue
            
#             # Avoid duplicates
#             journal_key = tuple(sorted(journal.getIds()))
#             if journal_key not in seen:
#                 seen.add(journal_key)
#                 result.append(journal)
        
#         return result
    
