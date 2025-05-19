#Class implementation


class IdentifiableEntity(object):
    def __init__ (self, id):
        self.id= id 

    def getIds(self):
        result = []
        for identifier in self.id: #Qui assumi che self.id sia un iterabile (es. lista). Se id è un singolo identificatore (es. stringa o numero), questo genererà errore.
            result.append(identifier)
        result.sort()
        return result

class Journal(IdentifiableEntity):
    def __init__(self, id, title, language, publisher, seal,licence, apc, areas, categories):
        super().__init__(id)
        self.title= title 
        self.language= language
        self.publisher = publisher
        self.seal= seal 
        self.licence= licence
        self.apc = apc
        self.hasCategories= categories #qui è dove si esprime la relazione "has a"
        self.hasAreas= areas #qui è dove si esprime la relazione "has a"
    
    def getTitle(self):
        return self.title
    
    def getLanguage(self):
        result= []
        for language in self.language:
            result.append(language)
      
        return result
    
    def getPublisher(self):
        if isinstance (self.publisher, str) or self.publisher is None:
            return self.publisher
        else:
            raise ValueError("Publisher deve essere str o None")
        
    
    
    def getDOAJSeal(self): #return boolean
        return self.seal
    
    def getLicence(self):
        return self.licence
    
    def getApc(self): #return boolean
        return self.apc
    
    def getCategories(self):
        result= []
        for category in self.hasCategories:
            result.append(category)
        
        return result
    
    def getAreas(self):
        result= []
        for area in self.hasAreas:
            result.append(area)
        result.sort() #non credo funzioni perchè .sort() funziona solo in place
        return result
    

class Category(object):
    def __init__(self, quartile ):
        self.quartile= quartile

    def getQuartile(self):
        if isinstance (self.quartile, str) or self.quartile is None:
            return self.quartile
        else:
            raise ValueError ("Quartile deve essere stringa o None")
        
class Area(object):
    def __init__(self, name):
        self.name= name

    def __str__(self):

        return self.name
    

#Creiamo l'Handler
class Handler():
    def __init__(self):
        self.DbPathOrUrl= ""
    
    def getDbPathOrUrl(self):
        return self.DbPathOrUrl


    def setDbPathOrUrl(self, new_path):
        self.DbPathOrUrl = new_path
        return True


#creiamo l'UploadHandler

class UploadHandler(Handler):
    def __init__(self):
        super().__init__

    def PushDataToDb(DbPathOrUrl):
        

