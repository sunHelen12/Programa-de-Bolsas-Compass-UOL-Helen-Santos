# Biblioteca para o Hash
import hashlib

# Recebendo String
entrada = input("Digite algo: ")

# Gerando Hash por meio do SHA-1
algoritmo = hashlib.sha1(entrada.encode('utf-8'))

# Hash formatado
resultado = algoritmo.hexdigest()

# Imprimindo
print(f"Hash com SHA-1: {resultado}" )