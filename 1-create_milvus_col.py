import configparser
from pymilvus import MilvusClient, DataType

# Charger les informations de connexion
config = configparser.ConfigParser()
config.read('config.ini')
uri = config['zillizconnection']['URI']
api_key = config['zillizconnection']['API_KEY']

# Se connecter à l'instance Milvus
client = MilvusClient(uri=uri, token=api_key)

# Créer le schéma
schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=True)

# Ajouter les champs au schéma
schema.add_field(field_name="Auto_Id", datatype=DataType.INT64, is_primary=True, auto_id=True, description="Primary key")
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=256, description="Label vector")
schema.add_field(field_name="Code_Ana", datatype=DataType.VARCHAR, max_length=20, description="Code analyse")
schema.add_field(field_name="Libelle_Ana", datatype=DataType.VARCHAR, max_length=50,enable_analyzer="True", enable_match="True", description="Frlis Label")
schema.add_field(field_name="Libelle_Llm", datatype=DataType.VARCHAR, max_length=50,enable_analyzer="True", enable_match="True", description="Nomalized Label")
schema.add_field(field_name="Iata_code", datatype=DataType.VARCHAR, max_length=40, description="Code SEL")
schema.add_field(field_name="Chap_Ana", datatype=DataType.VARCHAR, max_length=40, description="Chapitre")

# Préparer les paramètres d'index
index_params = client.prepare_index_params()

# Ajouter les index
index_params.add_index(
    field_name="Auto_Id",
    index_type="STL_SORT"
)

index_params.add_index(
    field_name="vector",
    index_type="HNSW",
    metric_type="COSINE",
    params={"M": 32, "efConstruction": 400, "efSearch": 150}
)

# Créer la collection avec l'index chargé simultanément
client.create_collection(
    collection_name="FRLISNAQ",
    schema=schema,
    index_params=index_params
)

# Vérifier l'état de chargement de la collection
res = client.get_load_state(collection_name="FRLISNAK")
print(res)
