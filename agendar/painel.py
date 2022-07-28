from kafka import TopicPartition, KafkaConsumer
from time import sleep
import json

painel_agendar = KafkaConsumer(
    bootstrap_servers=["kafka:29092"],
    api_version=(0, 10, 1),

    auto_offset_reset="earliest",
    consumer_timeout_ms=1000)

topico = TopicPartition("agendar", 0)
painel_agendar.assign([topico])

painel_agendar.seek_to_beginning(topico)
while True:
    print("aguardando resposta de agendamentos...")

    for solicitacao in painel_agendar:
        dados_da_solicitacao = json.loads(solicitacao.value)
        print(f"dados da solicitacao: {dados_da_solicitacao}")

    sleep(4)