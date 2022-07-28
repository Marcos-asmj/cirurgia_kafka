from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import json

PROCESSO = "agendar"
PROCESSO_QUARTO = "quarto"

def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()


def agendar_cirurgia(dados_da_solicitacao):
    valido, mensagem = False, ""
    if dados_da_solicitacao["sucesso"] == 1:
        valido = True
        mensagem = "Cirurgia agendada com sucesso!"
    
    return valido, mensagem


def executar():
    global deslocamento

    consumidor_de_solicitacao = KafkaConsumer(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1),
        auto_offset_reset = "earliest",
        consumer_timeout_ms = 1000
    )
    topico = TopicPartition(PROCESSO_QUARTO, 0)
    consumidor_de_solicitacao.assign([topico])
    consumidor_de_solicitacao.seek(topico, deslocamento)

    for solicitacao in consumidor_de_solicitacao:
        deslocamento = solicitacao.offset + 1

        dados_da_solicitacao = solicitacao.value
        dados_da_solicitacao = json.loads(dados_da_solicitacao)

        valido, mensagem = agendar_cirurgia(dados_da_solicitacao)
        
        if valido:
            dados_da_solicitacao["sucesso"] = 1
            dados_da_solicitacao["mensagem"] = mensagem
        else:
            dados_da_solicitacao["sucesso"] = 0

        try:
            produtor = KafkaProducer(
                bootstrap_servers = [ "kafka:29092" ],
                api_version = (0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value= json.dumps(dados_da_solicitacao).encode("utf-8"))
        except KafkaError as erro:
             print(f"ocorreu um erro: {erro}")

if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar, trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
