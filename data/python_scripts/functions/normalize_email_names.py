import re
import unicodedata                                                                
 
# Função para normalizar nomes em emails
def normalize_email_names(text: str) -> str:
    
    # Remove espaços e transforma todas as letras em minúsculas
    s = text.strip().lower()
    
    print(s)
    
    s = unicodedata.normalize('NFKD', s)
    
    print(s)
    
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    
    # remove caracteres não-ascii (ex.: grego)
    s = s.encode("ascii", "ignore").decode("ascii")
    
    # mantém só [a-z0-9]
    s = re.sub(r"[^a-z0-9]+", "", s)
    
    return s

print(normalize_email_names('João'))