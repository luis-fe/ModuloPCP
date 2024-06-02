FROM python:3.9-slim

# Instale as dependências do sistema necessárias e o OpenJDK 11
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie e instale as dependências do Python
COPY requeriments.txt requeriments.txt

# Mostrar versão do pip e instalar dependências
RUN pip --version \
    && pip install --no-cache-dir -r requeriments.txt

COPY . .

# Defina a variável de ambiente para não criar bytecode (.pyc) do Python
ENV PYTHONDONTWRITEBYTECODE=1

# Defina a variável de ambiente para que o output do Python não seja bufferizado
ENV PYTHONUNBUFFERED=1

# Exponha a porta em que a aplicação Flask estará rodando
EXPOSE 5000

# Comando para rodar a aplicação Flask
CMD ["python", "app.py"]
