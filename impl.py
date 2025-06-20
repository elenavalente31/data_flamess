#Class implementation


# Implementiamo le classi che vediamo nell'UML:

class IdentifiableEntity:
    def __init__(self, identifiers):
        self.id = set() # ci possono essere più id [1..*]
        for identifier in identifiers:
            self.id.add(identifier)

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result

class Journal(IdentifiableEntity):
    def __init__(self, identifiers, title, languages, seal: bool, licence, apc: bool, publisher=None):
        super().__init__(identifiers) 
        self.title = title
        self.languages = languages   # perché se metto lista mi divide la stringa in caratteri
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc
        self.categories = []  
        self.areas= []    


    # Metodi
    def getTitle(self):
        return self.title

    def getLanguages(self):
        return sorted(self.languages)  # Restituisce lista ordinata

    def getPublisher(self):
        return self.publisher

    def hasDOAJSeal(self):
        return self.seal

    def getLicence(self):
        return self.licence

    def hasAPC(self):
        return self.apc  # IL RISULTATO DEVE ESSERE UN BOOLEAN
    
    # Metodi per le relazioni
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

# (qui potrei anche evitare il super perché tanto oltre a identifiers non c'è niente. In category c'è invece quartile.)


# Gestione del caricamento dei dati (JSON -> SQL)

class Handler():
    
    def __init__(self):

        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl): # crea l'attributo dbpath
        self.dbPathOrUrl = pathOrUrl



# se non scrivo init sotto uploadhandler viene ereditato dalla classe handler 
# (non ci serve, ma volendo potrei modificare metodi di handler in uploadhandler)
# qui non scrivo super


class UploadHandler(Handler):

    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        pass

# in python esiste il concetto di classe astratta, cioè una classe che non deve essere inizializzata, ma che fa da modello per altre classi.
# Questa non è una classe astratta, però siccome sto ereditando questa funzione nelle successive non serve scrivere niente dentro.



# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# MARI

# Dal JSON al db relazionale

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
        
# Inizializzare conn a None prima del blocco try è una pratica di programmazione standard e molto importante per garantire la robustezza e la correttezza del tuo codice, specialmente quando lavori con risorse esterne come le connessioni a un database.
# I motivi principali sono:

#  1. Gestione degli Errori nelle Prime Fasi:
#     Se si verifica un errore molto presto nel blocco try (ad esempio, il database path db_path non è valido o c'è un problema di permessi che impedisce a sqlite3.connect(db_path) di stabilire la connessione), la variabile conn potrebbe non essere mai assegnata.
#     Senza conn = None all'inizio, se la connessione fallisce, conn non esisterà.
#     Quando il controllo passa al blocco finally, il codice if conn: cercherebbe di accedere a una variabile non definita, causando un NameError. Inizializzandola a None, garantisci che conn esista sempre, anche se l'assegnazione successiva fallisce.

#  2. Chiusura Sicura della Connessione (nel finally):
#     Il blocco finally è progettato per eseguire codice che deve sempre essere eseguito, indipendentemente dal fatto che si sia verificato un errore nel blocco try o meno. La sua funzione principale qui è chiudere la connessione al database (conn.close()).
#     Il controllo if conn: all'interno del finally è essenziale. Se la connessione non è stata stabilita con successo (quindi conn è rimasto None), non puoi cercare di chiamare conn.close() su un oggetto None, altrimenti otterresti un AttributeError.
#     Inizializzando conn = None e usando if conn: nel finally, ti assicuri di tentare di chiudere la connessione solo se è stata effettivamente aperta con successo.

        # Connessione al database
    
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        try:

            # Creazione tabelle
            # -- Tabella principale dei Journal
            cursor.execute('''CREATE TABLE IF NOT EXISTS Journal (
            internal_id TEXT PRIMARY KEY);''')

            # -- Identificatori alternativi per un Journal
            cursor.execute('''CREATE TABLE IF NOT EXISTS JournalIdentifier (
            journal_id TEXT NOT NULL,
            identifier TEXT NOT NULL,
            PRIMARY KEY (journal_id, identifier),
            FOREIGN KEY (journal_id) REFERENCES Journal(internal_id));''')

            # -- Categorie (es. Hematology, Medicine, ecc.)
            cursor.execute('''CREATE TABLE IF NOT EXISTS Category (
            category_id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            quartile TEXT NOT NULL,
            UNIQUE(category, quartile)
            );''') # UNIQUE(category, quartile) significa che non possono esistere due righe diverse 
            # nella tabella Category che abbiano contemporaneamente gli stessi valori nelle colonne category e quartile.

            # -- Aree (es. Computer Science, Medicine, ecc.)
            cursor.execute('''CREATE TABLE IF NOT EXISTS Area (
            area_id TEXT PRIMARY KEY,
            area TEXT NOT NULL
            );''')

            # -- Associazione molti-a-molti: Journal ha una o più Categorie
            cursor.execute('''CREATE TABLE IF NOT EXISTS HasCategory (
            journal_id TEXT NOT NULL,
            category_id TEXT NOT NULL,
            PRIMARY KEY (journal_id, category_id),
            FOREIGN KEY (journal_id) REFERENCES Journal(internal_id),
            FOREIGN KEY (category_id) REFERENCES Category(category_id)
            );''')

            # -- Associazione molti-a-molti: Journal ha una o più Aree
            cursor.execute('''CREATE TABLE IF NOT EXISTS HasArea (
            journal_id TEXT NOT NULL,
            area_id TEXT NOT NULL,
            PRIMARY KEY (journal_id, area_id),
            FOREIGN KEY (journal_id) REFERENCES Journal(internal_id),
            FOREIGN KEY (area_id) REFERENCES Area(area_id)
            );''')


            # CARICAMENTO JSON

            # Lettura del file:
            
            with open(path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                    

            # --- Recupero dei contatori esistenti --- 
            # è fondamentale recuperare l'ultimo ID esistente nel database prima di iniziare a generare nuovi ID. 
            # Questo garantisce che i nuovi ID siano unici e non sovrappongano quelli già presenti.

            cursor.execute("SELECT IFNULL(MAX(CAST(SUBSTR(internal_id, INSTR(internal_id, '-') + 1) AS INTEGER)), -1) FROM Journal")
            journal_counter = cursor.fetchone()[0] + 1
            
            cursor.execute("SELECT IFNULL(MAX(CAST(SUBSTR(category_id, INSTR(category_id, '-') + 1) AS INTEGER)), -1) FROM Category")
            cat_counter = cursor.fetchone()[0] + 1

            cursor.execute("SELECT IFNULL(MAX(CAST(SUBSTR(area_id, INSTR(area_id, '-') + 1) AS INTEGER)), -1) FROM Area")
            area_counter = cursor.fetchone()[0] + 1
            # ----------------------------------------

            for journal_entry in json_data:
            # Verifica se il journal esiste già basandosi su uno dei suoi identificatori unici (es. ISSN/EISSN).
            # Questo è un miglioramento importante per evitare di duplicare un intero journal
            # se lo stesso journal appare in più file JSON o in esecuzioni successive.
            # Si assume che 'identifiers' sia presente nel JSON e che almeno uno sia sufficiente per l'unicità.
            # Se non ci sono identificatori nel JSON, la logica di fallback si baserà solo sul `journal_counter`,
            # che è meno robusto per evitare duplicati del journal stesso.
                journal_identifiers = journal_entry.get('identifiers', []) # Estrae la lista degli identificatori dal JSON
                existing_journal_id = None # Inizializza la variabile per memorizzare l'ID del journal esistente.
                
                # Controlla TUTTI gli identificatori per vedere se il journal esiste già nel database.
                if journal_identifiers: # Ovviamente ci sono identificatori da controllare perché è obbligatorio avere degli identifiers, ma per sicurezza aggiungo questo if.
                    for identifier_to_check in journal_identifiers: # Itera su ogni identificatore presente nella lista.
                        cursor.execute('''
                            SELECT Journal.internal_id
                            FROM Journal
                            JOIN JournalIdentifier ON Journal.internal_id = JournalIdentifier.journal_id
                            WHERE JournalIdentifier.identifier = ?
                        ''', (identifier_to_check,)) # Questa query cerca nei dati un journal che abbia quell'identifier.
                        result = cursor.fetchone() # Recupera la prima riga del risultato della query. Sarà `None` se non trova corrispondenze, altrimenti una tupla che contiene l'internal_id del journal (es. ('journal-123',)).
                        if result: # Se la query ha trovato una corrispondenza (cioè `result` non è `None`), cioè viene trovato un Journal con quell'id:
                            existing_journal_id = result[0] # Assegna ad una variabile l'internal_id del journal trovato.
                            break # il ciclo sugli identificatori viene interrotto (break). Non è necessario controllare gli altri identificatori per questo journal, in quanto ne abbiamo già trovato uno corrispondente.

                if existing_journal_id: # Se è stato trovato un internal_id di un journal esistente:
                    current_journal_id = existing_journal_id # L'internal_id corrente del journal da elaborare è quello esistente.

                else: # Altrimenti, se `existing_journal_id` è rimasto `None` (cioè il journal non esiste ancora nel database).
                    current_journal_id = f'journal-{journal_counter}' # Crea un nuovo internal_id per il journal.
                    try: # Inizia un blocco try per gestire potenziali errori di integrità.
                        cursor.execute("INSERT INTO Journal (internal_id) VALUES (?)", (current_journal_id,)) # Tenta di inserire il nuovo journal con il suo rispettivo internal_id (es. journal-numero a caso).
                        journal_counter += 1 # Incrementa il contatore per il prossimo nuovo journal.

                    except sqlite3.IntegrityError: # Cattura l'eccezione se si verifica un errore di integrità (es. ID duplicato inatteso).
                        print(f"Warning: Journal ID '{current_journal_id}' already exists (unexpected). Skipping insertion of this journal.") # Stampa un avviso.
                        continue # Salta l'elaborazione del `journal_entry` corrente e passa al successivo.
                    # N.B. Questa clausola cattura un errore IntegrityError che potrebbe verificarsi se, per qualche ragione inattesa, 
                    # l'ID current_journal_id dovesse già esistere (sebbene il sistema di contatori dovrebbe evitarlo, 
                    # è una buona pratica difensiva). In tal caso, viene stampato un avviso e il ciclo passa alla prossima voce journal_entry (continue) 
                    # senza elaborare ulteriormente quella corrente, evitando un crash.

                # Una volta che il codice ha determinato il current_journal_id per la voce JSON che sta elaborando, procede ad associare a quel journal_id (journal-numero a cui si trova il contatore) tutti gli identifiers (cioè ISSN e EISSN) presenti nella lista journal_identifiers del JSON.

                # Inserimento Identificatori
                for identifier in journal_identifiers: # Itera su ogni identificatore del journal corrente.
                    cursor.execute('''
                        INSERT OR IGNORE INTO JournalIdentifier (journal_id, identifier)
                        VALUES (?, ?)
                    ''', (current_journal_id, identifier)) # Inserisce l'identificatore associato al journal. `INSERT OR IGNORE` evita duplicati se l'identificatore esiste già per quel journal.
                
                # Inserimento Categorie e associazione HasCategory
                for category_data in journal_entry.get('categories', []): # Itera su ogni elemento della lista 'categories' nel JSON.
                    category_name = sanitize(category_data.get('id', '')).strip() # Estrae e sanifica il nome della categoria. 
                    quartile = category_data.get('quartile', '').strip() # Estrae il quartile della categoria.

                    if not category_name: # Salta se il nome della categoria è vuoto dopo la sanificazione.
                        continue

                    cursor.execute("SELECT category_id FROM Category WHERE category = ? AND quartile = ?", (category_name, quartile)) # Esegue una query per vedere se esiste già una categoria con lo stesso nome (category) e quartile nel database. È fondamentale controllare entrambi i campi, perché la stessa categoria potrebbe avere quartili diversi.
                    existing_cat = cursor.fetchone() # Recupera la prima riga del risultato.
                    
                    if existing_cat: # Se la categoria (associata a quel quartile) esiste già:
                        cat_id_to_use = existing_cat[0] # Usa il category_id (cat-numero) esistente della categoria.
                    else: # Se invece la categoria (associata a quel quartile) non esiste:
                        cat_id_to_use = f'cat-{cat_counter}' # Crea un nuovo category_id per la categoria.

                        try: # Inizia un blocco try per gestire potenziali errori di integrità.
                            cursor.execute('''
                                INSERT INTO Category (category_id, category, quartile)
                                VALUES (?, ?, ?)
                            ''', (cat_id_to_use, category_name, quartile)) # Tenta di inserire la nuova categoria.
                            cat_counter += 1 # Incrementa il contatore per la prossima nuova categoria.

                        except sqlite3.IntegrityError: # Cattura l'eccezione se si verifica un errore di integrità.
                            print(f"Warning: Category ID {cat_id_to_use} or category/quartile combination already exists (unexpected). Skipping association for this category.")

                    # Inserimento nella tabella HasCategory per associare il journal alla categoria.
                    cursor.execute('''
                        INSERT OR IGNORE INTO HasCategory (journal_id, category_id)
                        VALUES (?, ?)
                    ''', (current_journal_id, cat_id_to_use)) # `INSERT OR IGNORE` evita duplicati se l'associazione esiste già.
                
                # Inserimento Aree e associazione HasArea
                for area_name_raw in journal_entry.get('areas', []): # Itera su ogni elemento della lista 'areas' nel JSON.
                    if isinstance(area_name_raw, str): # Verifica se il nome dell'area è una stringa.
                        # Qui utilizziamo isinstance a differenza di come abbiamo fatto per le category
                        # La differenza nel trattamento sta nella struttura attesa degli elementi all'interno delle liste JSON:
                        # Per le aree, ti aspetti una lista di stringhe. isinstance(str) è una guardia per assicurarsi che stai operando su stringhe
                        # Per le categorie, ti aspetti una lista di dizionari. L'accesso ai dati avviene tramite .get('chiave', ''), che è già un modo robusto per estrarre valori da un dizionario, gestendo l'assenza della chiave restituendo una stringa vuota. 
                        # Se l'elemento stesso non fosse un dizionario, l'errore si manifesterebbe a un livello più alto (quando cerchi di chiamare .get() su di esso).
                        safe_area_name = sanitize(area_name_raw.strip()) # Sanifica e pulisce il nome dell'area.
                        if not safe_area_name: # Se il nome dell'area non è una stringa valida, cioè è uns stringa vuota oppure lo è diventata dopo la sanificazione:
                            continue
                    else: # altrimenti, se il  nome dell'area non è una stringa:
                        continue # riprendi il loop. Il continue interrompe l'elaborazione e passa all'elemento successivo. 

                   
                    cursor.execute("SELECT area_id FROM Area WHERE area = ?", (safe_area_name,)) # Cerca un'area esistente con lo stesso nome.
                    existing_area = cursor.fetchone() # Recupera la prima riga del risultato.
                    
                    if existing_area: # Se l'area esiste già:
                        area_id_to_use = existing_area[0] # Usa l'area_id (area-numero) esistente dell'area.
                    else: # Se l'area non esiste:
                        area_id_to_use = f'area-{area_counter}' # Crea un nuovo area_id (area-numero) per l'area.

                        try: # Inizia un blocco try per gestire potenziali errori di integrità.
                            cursor.execute('''
                                INSERT INTO Area (area_id, area)
                                VALUES (?, ?)
                            ''', (area_id_to_use, safe_area_name)) # Tenta di inserire la nuova area.
                            area_counter += 1 # Incrementa il contatore per la prossima nuova area.

                        except sqlite3.IntegrityError: # Cattura l'eccezione se si verifica un errore di integrità.
                            print(f"Warning: Area ID '{area_id_to_use}' or area already exists (unexpected). Skipping association for this area.") # Stampa un avviso.
                            continue # Salta l'associazione per questa area e passa alla successiva.
                    
                    # Inserimento nella tabella HasArea per associare il journal all'area.
                    cursor.execute('''
                        INSERT OR IGNORE INTO HasArea (journal_id, area_id)
                        VALUES (?, ?)
                    ''', (current_journal_id, area_id_to_use)) # `INSERT OR IGNORE` evita duplicati se l'associazione esiste già.
                        
            # Commit delle modifiche al database.
            conn.commit()
            print(f"Data successfully loaded from {path} into database {db_path}.")

        except sqlite3.Error as e: # Cattura qualsiasi errore specifico di SQLite.
            print(f"Database error during loading: {e}") # Stampa l'errore.
            conn.rollback() # Esegue un rollback di tutte le operazioni in caso di errore per mantenere l'integrità del database.
        
        # Blocco finally che viene eseguito sempre, indipendentemente dal fatto che si sia verificato un errore o meno.
        finally:        
            conn.close() # Chiude la connessione al database.



# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# CRISTINA


import pandas as pd
import sqlite3


class QueryHandler(Handler):
    def __init__(self):

        super().__init__()
       


    def getById(self, id):
        pass


class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()



    def getById(self, identifier: str) -> pd.DataFrame:
        """
            Dato un identificatore (e.g., ISSN), restituisce un DataFrame contenente
            tutti i dati associati al Journal, con categorie e aree aggregate come liste di stringhe.
            - internal_id
            - identifier (tutti gli id concatenati)
            - category (lista di categorie)
            - quartile (lista di quartili)
            - area (lista di aree)

            Returns:
                pd.DataFrame: Un DataFrame con una singola riga per il journal.
        """
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Trova l'internal_id del journal associato all'identifier
            cursor.execute('''
                SELECT journal_id FROM JournalIdentifier WHERE identifier = ?
            ''', (identifier,))
            row = cursor.fetchone()
            if not row:
                return pd.DataFrame()

            journal_id = row[0]

            # Recupera tutti gli identifiers di quel journal, nell’ordine di inserimento
            cursor.execute('''
                SELECT identifier
                FROM JournalIdentifier
                WHERE journal_id = ?
                ORDER BY rowid
            ''', (journal_id,))
            identifiers = [r[0] for r in cursor.fetchall()]
            identifiers_str = '; '.join(identifiers)

            # Query completa per categoria e area
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
            '''
            df = pd.read_sql_query(query, conn, params=(journal_id,))

            if df.empty:
                return pd.DataFrame({
                    'internal_id': [journal_id],
                    'identifier': [identifiers_str],
                    'category': [[]],
                    'quartile': [[]],
                    'area': [[]]
                })

            # Crea coppie (category, quartile) per mantenere l'allineamento
            df['category_quartile'] = list(zip(df['category'], df['quartile']))

            # Aggregazione
            aggregated = df.groupby('internal_id').agg({
                'category_quartile': lambda x: list({(cat, q) for cat, q in x if cat is not None}),
                'area': lambda x: list(x.dropna().unique())
            }).reset_index()

            # Estrai liste separate da category_quartile
            aggregated['category'] = aggregated['category_quartile'].apply(lambda x: [cat for cat, _ in x])
            aggregated['quartile'] = aggregated['category_quartile'].apply(lambda x: [q for _, q in x])

            # Inserisci identifier e rimuovi la colonna temporanea
            aggregated.insert(1, 'identifier', identifiers_str)
            aggregated.drop(columns=['category_quartile'], inplace=True)

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
            query = "SELECT DISTINCT category FROM Category"
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
            query = "SELECT DISTINCT area FROM Area"
            df = pd.read_sql_query(query, conn)
            return df
        except sqlite3.Error as e:
            print(f"Database error in getAllAreas: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()

    def getCategoriesWithQuartile(self, quartiles: list[str]) -> pd.DataFrame:
        """
        Returns a DataFrame containing all categories with the specified quartiles.

        Args:
            quartiles (list[str]): A list of quartiles (e.g., ['Q1', 'Q2']) to filter by.
                                   If empty, returns all categories with their quartiles.

        Returns:
            pd.DataFrame: A DataFrame with 'category' and 'quartile' columns.
        """
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)

            base_query = "SELECT category, quartile FROM Category"

            if not quartiles:
                df = pd.read_sql_query(base_query, conn)
            else:
                placeholders = ','.join('?' * len(quartiles))
                query = f"{base_query} WHERE quartile IN ({placeholders})"
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
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            params = [] # Initialize parameters list

            # Step 1: Determine the area IDs based on the provided area names
            if not area_names:
                # If no area names are provided, we query for all distinct categories.
                query = """
                    SELECT DISTINCT C.category AS category
                    FROM Category C
                """
                # Note: No need to join with HasCategory/HasArea if we just want all *existing* categories
                # that *could* be assigned. If you only want categories that are *actually assigned to at least one journal*,
                # you'd need to join with HasCategory. Let's assume for now you want all categories that exist in the Category table.
                # If the intent is "all categories *that are linked to any journal in any area*", then the query below is correct.
                # For this specific request "without the dict", and "no repetitions", a simple SELECT DISTINCT category is suitable
                # unless the underlying logic *must* involve journals for filtering.
                # Let's adjust to be consistent with previous logic that implicitly meant "assigned to at least one journal".

                # Revised query for 'all areas specified' (i.e., all categories assigned to any journal)
                query = """
                    SELECT DISTINCT C.category AS category
                    FROM HasCategory HC
                    JOIN Category C ON HC.category_id = C.category_id
                """
                params = []
            else:
                # First, retrieve the actual area_ids from the Area table using the provided area_names.
                area_name_placeholders = ','.join(['?'] * len(area_names))
                area_id_lookup_query = f"SELECT area_id FROM Area WHERE area IN ({area_name_placeholders})"

                cursor = conn.cursor()
                cursor.execute(area_id_lookup_query, list(area_names))
                fetched_area_ids = {row[0] for row in cursor.fetchall()}
                cursor.close()

                if not fetched_area_ids:
                    return pd.DataFrame(columns=['category']) # Return with just 'category' column

                # Now, construct the main query using the fetched area_ids.
                # We select DISTINCT category names from journals linked to these areas.
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

            # The result is already a DataFrame with distinct categories
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
        db_path = self.getDbPathOrUrl()
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            params = [] # Initialize parameters list


            if not category_names:
                # If no category names are provided, query for all distinct areas.
                # Similar to the category method, this assumes you want areas actually linked to a journal.
                query = """
                    SELECT DISTINCT A.area AS area
                    FROM HasArea HA
                    JOIN Area A ON HA.area_id = A.area_id
                """
                params = []
            else:
                # First, retrieve the actual category_ids from the Category table using the provided category_names.
                category_name_placeholders = ','.join(['?'] * len(category_names))
                category_id_lookup_query = f"SELECT category_id FROM Category WHERE category IN ({category_name_placeholders})"

                cursor = conn.cursor()
                cursor.execute(category_id_lookup_query, list(category_names))
                fetched_category_ids = {row[0] for row in cursor.fetchall()}
                cursor.close()

                if not fetched_category_ids:
                    return pd.DataFrame(columns=['area']) # Return with just 'area' column

                # Now, construct the main query using the fetched category_ids.
                # We select DISTINCT area names from journals linked to these categories.
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

            # The result is already a DataFrame with distinct areas
            return df

        except sqlite3.Error as e:
            print(f"Database error in getAreasAssignedToCategories: {e}")
            return pd.DataFrame(columns=['area']) # Ensure correct column in case of error
        finally:
            if conn:
                conn.close()
    

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# SILVIA

# Dal CSV al Graph Database


from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import read_csv


class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        if not self.getDbPathOrUrl():
            return False

        # Leggo il csv:
        file_csv = read_csv(path, 
                          keep_default_na=False,
                          dtype={
                              "Journal title": "string",
                              "Journal ISSN (print version)": "string",
                              "Journal EISSN (online version)": "string",
                              "Languages in which the journal accepts manuscripts": "string",
                              "Publisher": "string",
                              "DOAJ Seal": "string",  # Convertiremo manualmente a boolean
                              "Journal license": "string",
                              "APC": "string"        # Convertiremo manualmente a boolean
                          })
    
        graph = Graph()
        
        # Definizione delle classi e proprietà RDF
        Journal = URIRef("https://schema.org/Periodical")
        title = URIRef("https://schema.org/name")
        identifier = URIRef("https://schema.org/identifier")
        language = URIRef("https://schema.org/inLanguage")
        publisher = URIRef("https://schema.org/publisher")
        seal = URIRef("https://www.wikidata.org/wiki/Q73548471")
        license = URIRef("https://schema.org/license")
        apc = URIRef("https://www.wikidata.org/wiki/Q15291071") 

       
        base_url = "https://comp-data.github.io/res/"

        journal_id_dict = {}

        for idx, row in file_csv.iterrows():
        
            # Crea l'URI per il journal
            local_id = "journal-" + str(idx)
            subj = URIRef(base_url+ local_id)
            
            # Aggiunge il tipo del journal
            graph.add((subj, RDF.type, Journal)) 
            
            # Aggiunge le proprietà
            if row["Journal title"]:
                title_value = row["Journal title"]
                graph.add((subj, title, Literal(title_value)))

            
            identifiers = []

            if row["Journal ISSN (print version)"]:
                issn_value = row["Journal ISSN (print version)"]
                identifiers.append(issn_value)
                journal_id_dict[issn_value] = subj

            if row["Journal EISSN (online version)"]:
                eissn_value = row["Journal EISSN (online version)"]
                identifiers.append(eissn_value)
                journal_id_dict[eissn_value] = subj

            # Se almeno uno dei due identificatori è presente, aggiungilo
            if identifiers:
                combined_identifier = "; ".join(identifiers)
                graph.add((subj, identifier, Literal(combined_identifier)))
             
            # Languages 
            if row["Languages in which the journal accepts manuscripts"]:
                languages_str = row["Languages in which the journal accepts manuscripts"]
                languages_list = languages_str.split(",")
                for lang in languages_list:
                    clean_lang = lang.strip()
                    if clean_lang:  
                        graph.add((subj, language, Literal(clean_lang)))

            # Publisher
            if row["Publisher"]:
                publisher_value = row["Publisher"]
                graph.add((subj, publisher, Literal(publisher_value)))
            
            # DOAJ Seal (convertito in booleano)
            if row["DOAJ Seal"]:
                doaj_value = True if row["DOAJ Seal"].lower() == "yes" else False
                graph.add((subj, seal, Literal(doaj_value)))
            
            # Journal license
            if row["Journal license"]:
                license_value = row["Journal license"]
                graph.add((subj, license, Literal(license_value)))
            
            # APC (convertito in booleano)
            if row["APC"]:
                apc_value = True if row["APC"].lower() == "yes" else False
                graph.add((subj, apc, Literal(apc_value)))

       
        
        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl()
        
        
        store.open((endpoint, endpoint))
        data = graph.serialize(format="nt")
        query = f"INSERT DATA {{\n{data}\n}}"
        store.update(query)


        #for triple in graph.triples((None, None, None)):
        #    store.add(triple)
        store.close()

        return True




# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------




# ELENA
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get



# Definizione class QueryHandler e JournalQueryHandler


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
            return pd.DataFrame() #if there's no value specified as id, or if it's empty, returns an empty DataFrame

        filter_id = f 'FILTER (
            STR(?identifier) = "{id}" ||
            STRSTARTS(STR(?identifier), "{id}; ") ||
            STRENDS(STR(?identifier), "; {id}")
         
        )'
        
        #applies the id filter to the basic query
        
        query = self.PREFIXES + self.BASE_QUERY.format(filter=filter_id) #final query of the method, that contains prefixes, base query and the specific filter
        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)
        return df

    

    def getAllJournals(self):

        #here there's no filter applied to the basic query. the method just returns all the journals as they are stored in the database

        query= self.PREFIXES + self.BASE_QUERY.format(filter="")

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)
            
        return df

    
    
    def getJournalsWithTitle(self, partialTitle):

        #filter_title is the specific filter for this method. It basically assures that the partialTitle specified will match perfectly and/or partially
        
        filter_title= f'FILTER(CONTAINS(LCASE(?title), LCASE("{partialTitle}")))' 
        
        query= self.PREFIXES + self.BASE_QUERY.format(filter= filter_title)  #the filter gets applied to the final query of the method

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

        

        ####escaped_title = partialTitle.replace('\\', '\\\\').replace('"', '\\"')
        #### e poi nel FILTER si inserisce {escapedTitle} 
        #da valutare?
    

    def getJournalsPublishedBy(self, partialName):

        if not partialName:
            
            return pd.DataFrame() #if there is not a value in input, or if there's an empty value, returns an empty DataFrame

        #filter_publisher is the specific filter for this method. It basically assures that the partialName specified will match perfectly and/or partially
        
        filter_publisher = f'FILTER(CONTAINS(LCASE(?publisher), LCASE("{partialName}")))'

        query= self.PREFIXES + self.BASE_QUERY.format(filter=filter_publisher)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df
    

    
    def getJournalsWithLicense(self,license):
        if not license:
            return self.getAllJournals()
        
        license = license.strip().upper()
    
        # Crea un filtro che cerca:
        # 1. Licenza esatta (es. "CC BY")
        # 2. Licenza all'inizio della stringa (es. "CC BY, CC BY-ND")
        # 3. Licenza nel mezzo (es. "CC BY-ND, CC BY")
        # 4. Licenza alla fine (es. "CC BY-NC, CC BY")
        filter_license = f'
            FILTER(
                STR(?license) = "{license}" ||
                STRSTARTS(STR(?license), "{license}, ") ||
                CONTAINS(STR(?license), ", {license}, ") ||
                STRENDS(STR(?license), ", {license}")
            )'
        query= self.PREFIXES + self.BASE_QUERY.format(filter=filter_license)
        
        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)  #creates a DataFrame with the values of ?license
        

        return df
    
    

    def getJournalsWithAPC(self):

        filter_tapc= f'FILTER(LCASE(STR(?apc)) = "true")'  #creates the filter for the boolean "true" 
        
        query= self.PREFIXES + self.BASE_QUERY.format(filter= filter_tapc) #applies it to the final query of the method
        
        endpoint = self.getDbPathOrUrl()
        
        df = get(endpoint, query, True)

        return df
        


    def getJournalsWithoutAPC(self): 

        filter_fapc = f'FILTER(LCASE(STR(?apc)) = "false")'  #creates the filter for the boolean "false"
       
        query= self.PREFIXES + self.BASE_QUERY.format(filter=filter_fapc)  #applies it to the final query of the method
        
        endpoint = self.getDbPathOrUrl()
        
        df = get(endpoint, query, True)

        return df
    


    def getJournalsWithDOAJSeal(self):
     
        filter_seal= f'FILTER(LCASE(STR(?seal)) = "true")' #creates the filter for the boolean value "true"
        
        query = self.PREFIXES+ self.BASE_QUERY.format(filter=filter_seal) #applies it to the final query of the method

        endpoint = self.getDbPathOrUrl()
        
        df = get(endpoint, query, True)

        return df


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

