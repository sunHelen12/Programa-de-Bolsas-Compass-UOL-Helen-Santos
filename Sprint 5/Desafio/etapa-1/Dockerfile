# Imagem oficial e leve do Python
FROM python:3.9-slim

# Criando uma pasta dentro do container para organizar arquivos
WORKDIR /app

# Usando a lista de requerimentos para instalar as dependências
COPY etapa-1/dependencias.txt .
RUN pip install --no-cache-dir -r dependencias.txt

# Copiando os dados para dentro do container
COPY arquivos/ ./arquivos/
# Copiando o script para dentro de '/app'
COPY etapa-1/etapa_1.py .

# Definindo o comando que será executado quando o container iniciar
CMD ["python", "etapa_1.py"]