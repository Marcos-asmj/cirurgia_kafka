from kafka import TopicPartition, KafkaConsumer
from time import sleep
import json

painel_solicitacao = KafkaConsumer(
    bootstrap_servers=["kafka:29092"],
    api_version=(0, 10, 1),

    auto_offset_reset="earliest",
    consumer_timeout_ms=1000)

topico = TopicPartition("solicitar", 0)
painel_solicitacao.assign([topico])

painel_solicitacao.seek_to_beginning(topico)
while True:
    print("aguradando solicitacoes de cirurgia...")

    for solicitacao in painel_solicitacao:
        dados_da_solicitacao = json.loads(solicitacao.value)
        print(f"dados da solicitacao: {dados_da_solicitacao}")

    sleep(4)