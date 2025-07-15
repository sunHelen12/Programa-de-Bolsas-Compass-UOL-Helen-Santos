# EX04 - Escreva uma função que recebe uma lista e retorna uma nova lista sem 
# elementos duplicados. Utilize a lista a seguir para testar sua função.
# ['abc', 'abc', 'abc', '123', 'abc', '123', '123']

def semDuplicacao(duplicatas):
  return list(set(duplicatas))

if __name__ == '__main__':
  duplicatas = ['abc', 'abc', 'abc', '123', 'abc', '123', '123']
  print(semDuplicacao(duplicatas))