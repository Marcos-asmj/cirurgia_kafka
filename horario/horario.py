from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import json

PROCESSO = "horario"
PROCESSO_SOLICITACAO = "solicitar"


def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()


HORARIOS = "./horarios.json"


def checar_disponibilidade(dados_da_solicitacao):

    valido, data, horario = False, "", ""

    with open(HORARIOS, "r") as arquivo_horarios:
        horarios = json.load(arquivo_horarios)
        dias = horarios["dias"]
        for dia in dias:
            if ((dados_da_solicitacao["dia"] == dia["id"]) and (dados_da_solicitacao["horario"] == 1) and (dia["qtd_manha"] < 2)):
                valido = True
                data = dia["dia"]
                horario = "manha"
                dia["qtd_manha"] += 1
                break
            else:
                if ((dados_da_solicitacao["dia"] == dia["id"]) and (dados_da_solicitacao["horario"] == 2) and (dia["qtd_tarde"] < 2)):
                    valido = True
                    data = dia["dia"]
                    horario = "tarde"
                    dia["qtd_tarde"] += 1
                    break

        arquivo_horarios.close()

    return valido, data, horario


def executar():
    global deslocamento

    consumidor_de_solicitacoes = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000
    )
    topico = TopicPartition(PROCESSO_SOLICITACAO, 0)
    consumidor_de_solicitacoes.assign([topico])
    consumidor_de_solicitacoes.seek(topico, deslocamento)

    for solicitacao in consumidor_de_solicitacoes:
        deslocamento = solicitacao.offset + 1

        dados_do_solicitacao = json.loads(solicitacao.value)
        valido, data, horario = checar_disponibilidade(dados_do_solicitacao)
        if valido:
            dados_do_solicitacao["sucesso"] = 1
            dados_do_solicitacao["dia"] = data
            dados_do_solicitacao["horario"] = horario
        else:
            dados_do_solicitacao["sucesso"] = 0

        try:
            produtor = KafkaProducer(
                bootstrap_servers=["kafka:29092"],
                api_version=(0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value=json.dumps(dados_do_solicitacao).encode("utf-8"))
        except KafkaError as erro:
            print(f"ocorreu um erro: {erro}")


if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar, trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
