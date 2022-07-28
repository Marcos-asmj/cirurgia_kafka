from kafka import TopicPartition, KafkaConsumer
from time import sleep
import json

painel_quarto = KafkaConsumer(
    bootstrap_servers=["kafka:29092"],
    api_version=(0, 10, 1),

    auto_offset_reset="earliest",
    consumer_timeout_ms=1000)

topico = TopicPartition("quarto", 0)
painel_quarto.assign([topico])

painel_quarto.seek_to_beginning(topico)
while True:
    print("aguardando verificacao de quarto...")

    for solicitacao in painel_quarto:
        dados_da_solicitacao = json.loads(solicitacao.value)
        print(f"dados da solicitacao: {dados_da_solicitacao}")

    sleep(4)