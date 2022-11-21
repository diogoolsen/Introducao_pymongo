
########
# Breve introdução ao pymongo
# Diogo Olsen - 18/11/2022
# diogo.olsen@ifpr.edu.br
########

# Importa a biblioteca Pymongo
# A biblioteca deve estar instalada no sistema
# O MongoDB deve estar instalado e executando no sistema
import pymongo

# Inicializa o cliente pymongo para conectar no MongoDB
client = pymongo.MongoClient('localhost', 27017)

# Acessa a base de dados "my_database"
# Caso ela não exista, é criada
#
# Esta operação pode ser feita com duas sintaxes
# data_base = client.my_database
data_base = client['my_database']

# Acessa a coleção "my_collection"
# Caso a coleção não exista, é criada
#
# Esta operação pode ser feita com duas sintaxes
# collection = data_base.my_collection
collection = data_base['my_collection']

# Ou os três passos acima podem ser concatenados em uma linha única
# collection = pymongo.MongoClient('localhost',
#       27017)['my_database']['my_collection']


########
# Trabalhando com um documento de cada vez
########
print('\nTRABALHANDO COM UM DOCUMENTO DE CADA VEZ\n')


########
# Inserindo dados no BD
########
print('\nINSERINDO DADOS NO BD\n - insert_one\n')

# Cria um dicionário para inserir seus dados no BD
usuario = {'nome': 'Diogo', 'tipo': 'Servidor'}

# insert_one
#
# Insere um documento baseado no dicionário ao BD
collection.insert_one(usuario)
# Ou insere diretamente o dicionário no BD sem usar uma variável temporária
doc = collection.insert_one({'titulo': 'Duna',
                             'ISBN': '123456',
                             'capa': 'dura'})

# O retorno da função insert_one pode ser consultada da seguinte forma:
#
# inserted_id - ID do documento inserido
print('\tID do documento inserido no BD:', doc.inserted_id)
# acknowledged -Confirmação da inserção
print('\tDocumento inserido no BD [True/False]:', doc.acknowledged)


########
# Buscando dados no BD
########
print('\nBUSCANDO DADOS NO BD\n - find_one\n')

# find_one
#
# Busca o primeiro documento que coincidir com os dados buscados
#
# Prepara um dicionário com os campos a serem buscados
consulta = {'nome': 'Diogo'}
# Realiza a busca
doc = collection.find_one(consulta)
print('\tDocumento encontrado:', doc)
print('\tTipo do dado retornado:', type(doc))

# Podem ser passadas diversos pares chave/valor para a busca
print('\tBusca por vários campos:', collection.find_one({'titulo': 'Duna',
                                                         'ISBN': '123456'}))
# Caso não seja encontrado nenhum dado, retorna None
print('\tBusca dados inexistentes:', collection.find_one({'nome': 'PAULO'}))
# A busca também pode ser realizada pelo '_id' do documento
doc_id = doc['_id']
doc = collection.find_one({'_id': doc_id})
print('\tBuscando pelo _id:', doc_id, '- Doc encontrado:', doc)


########
# Atualizando dados no BD
########
print('\nATUALIZANDO DADOS NO BD\n - update_one\n')

# update_one
#
# Prepara a consulta e o novo valor
consulta = {'nome': 'Diogo'}
# O $set recebe um dicionário com as atualizações que devem ser realizadas
# Caso a chave já exista (como no 'nome') ela é atualizada
# Caso a chave não exista (como em 'data_nas') ela é criada
novo_valor = {'$set': {'nome': 'Diogo R. Olsen', 'data_nas': '12/01/1984'}}
# Executa a atualização
result = collection.update_one(consulta, novo_valor)

# O retorno da função update_one pode ser consultada da seguinte forma:
#
# matched_count
#  - Quantidade de documentos ENCONTRADOS com o dicionário de consulta
print('\tDocumentos encontrados:', result.matched_count)
# modified_count
#  - Quantidade de documentos MODIFICADOS com o dicionário de consulta
print('\tDocumentos atualizados:', result.modified_count)
# acknowledged
#  - Confirmação da execução da atualização - Não confirma a atualização e sim
#    a execução da função
print('\tConfirmação da atualização:', result.acknowledged)


########
# Atualizando dados no BD
########
print('\nAPAGANDO DADOS NO BD\n - delete_one\n')

# delete_one
#
# Prepara a consulta
consulta = {'nome': 'Diogo R. Olsen'}
# Executa a deleção
result = collection.delete_one(consulta)

# O retorno da função delete_one pode ser consultada da seguinte forma:
#
# deleted_count
#  - Quantidade de documentos Deletados com o dicionário de consulta
print('\tDocumentos deletados:', result.deleted_count)
# acknowledged
#  - Confirmação da execução da deleção - Não confirma a deleção e sim
# a execução da função
print('\tConfirmação da deleção:', result.acknowledged)


########
# Trabalhando com múltiplos documentos
########
print('\n\nTRABALHANDO COM MÚLTIPLOS DOCUMENTOS\n')


########
# Inserindo dados no BD
########
print('\nINSERINDO DADOS NO BD\n - insert_many\n')

# Cria uma lista de dicionários para inserir seus dados no BD
lista_de_usuarios = [
    {'nome': 'Diogo', 'tipo': 'Professor'},
    {'nome': 'Fernando', 'tipo': 'Estudante'},
    {'nome': 'Rafael', 'tipo': 'Estudante'},
    {'nome': 'Arnaldo', 'tipo': 'Estudante'}
]

# insert_many
#
# Insere um conjunto de documentos baseado nos dicionários ao BD
result = collection.insert_many(lista_de_usuarios)

# O retorno da função insert_many pode ser consultada da seguinte forma:
#
# inserted_ids - Lista ('pymongo.results.InsertManyResult')
# de _id dos documentos inseridos no BD
print('\tIDs dos documentos inseridos:', result.inserted_ids)
print('\tTipo de retorno de insert_many:', type(result))
# Iterando sobre os IDs inseridos
for doc_id in result.inserted_ids:
    print('\tDocs:', doc_id)


########
# Buscando dados no BD
########
print('\nBUSCANDO DADOS NO BD\n - find\n')

# find
#
# Busca o conjunto de documentos que coincide com a busca
#
# Prepara um dicionário com os campos a serem buscados
consulta = {'tipo': 'Estudante'}
# Realiza a busca
# O find retorna um cursor com os dados
cursor = collection.find(consulta)

# Iterar sobre o cursor contendo os dados encontrados
for doc in cursor:
    print('\t', doc)

print('\n\tProjection\n')

# Pode usar projeção
cursor = collection.find(consulta, {'nome': 1, '_id': 0})

# Iterar sobre o cursor contendo os dados encontrados
for doc in cursor:
    print('\t\t', doc)

print('\n\tLimit\n')

# Pode limitar a busca
cursor = collection.find(consulta, {'nome': 1, '_id': 0}).limit(2)

# Iterar sobre o cursor contendo os dados encontrados
for doc in cursor:
    print('\t\t', doc)

print('\n\tSort\n')

# Pode ordenar a busca
cursor = collection.find(consulta, {'nome': 1, '_id': 0}).limit(2)\
    .sort('nome', pymongo.ASCENDING)

# Iterar sobre o cursor contendo os dados encontrados
for doc in cursor:
    print('\t\t', doc)


########
# Contando documentos no BD
########

print('\nCONTANDO DOCUMENTOS NO BD\n - count_documents\n')

# count_documents
#
# Conta quantos documentos coincidem com os dados buscados
#
# Prepara um dicionário com os campos a serem buscados
consulta = {'tipo': 'Estudante'}
# Realiza a contagem
result = collection.count_documents(consulta)

print('\tDocumentos encontrados:', result)


########
# Atualizando um conjunto de documentos no BD
########

print('\nATUALIZANDO UM CONJUNTO DE DOCUMENTOS NO BD\n - update_many\n')

# update_many
#
# Atualiza um conjunto de documentos que coincidem com os dados buscados
#
# Prepara um dicionário com os campos a serem buscados
consulta = {'tipo': 'Estudante'}
# Prepara um dicionário com os campos a serem alterados
novo_valor = {'$set': {'tipo': 'Aluno'}}
# Realiza a atualização
result = collection.update_many(consulta, novo_valor)

# matched_count
#  - Quantidade de documentos ENCONTRADOS com o dicionário de consulta
print('\tDocumentos encontrados:', result.matched_count)
# modified_count
#  - Quantidade de documentos MODIFICADOS com o dicionário de consulta
print('\tDocumentos atualizados:', result.modified_count)
# acknowledged
#  - Confirmação da execução da atualização - Não confirma a atualização e sim
#    a execução da função
print('\tConfirmação da atualização:', result.acknowledged)

# Verificando a alteralção
cursor = collection.find({'tipo': 'Aluno'})

print('\tDocumentos alterados:')

# Iterar sobre o cursor contendo os dados encontrados
for doc in cursor:
    print('\t\t', doc)


########
# Deletando um conjunto de documentos no BD
########
print('\nDELETANDO UM CONJUNTO DE DOCUMENTOS NO BD\n - delete_many\n')

# delete_many
#
# Deleta um conjunto de documentos coincidem com os dados buscados
#
# Prepara um dicionário com os campos a serem buscados
consulta = {'tipo': 'Aluno'}

# Deleta dos dados
result = collection.delete_many(consulta)

# O retorno da função delete_many pode ser consultada da seguinte forma:
#
# deleted_count
#  - Quantidade de documentos Deletados com o dicionário de consulta
print('\tDocumentos deletados:', result.deleted_count)
# acknowledged
#  - Confirmação da execução da deleção - Não confirma a deleção e sim
#    a execução da função
print('\tConfirmação da deleção:', result.acknowledged)


# ATENÇÃO - deletando tudo na base
result = collection.delete_many({})

for doc in collection.find():
    print(doc)
