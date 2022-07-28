from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import json

PROCESSO = "quarto"
PROCESSO_HORARIO = "horario"

def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

QUARTOS = "./quartos.json"

def checar_quartos(dados_da_solicitacao):
    
    valido, dia, horario, leito = False, "", 0, 0
    if dados_da_solicitacao["sucesso"] == 1:
        with open(QUARTOS, "r") as arquivo_quartos:
            tabela = json.load(arquivo_quartos)
            quartos = tabela["quartos"]
            for quarto in quartos:
                if ((dados_da_solicitacao["dia"] == "Segunda") and (quarto["Segunda"] == 0)):
                    valido = True
                    dia = dados_da_solicitacao["dia"]
                    horario = dados_da_solicitacao["horario"]
                    leito = quarto["id"]
                    quarto["Segunda"] += 1
                    break
                else:
                    if ((dados_da_solicitacao["dia"] == "Terca") and (quarto["Terca"] == 0)):
                        valido = True
                        dia = dados_da_solicitacao["dia"]
                        horario = dados_da_solicitacao["horario"]
                        leito = quarto["id"]
                        quarto["Terca"] += 1
                        break
                    else:
                        if ((dados_da_solicitacao["dia"] == "Quarta") and (quarto["Quarta"] == 0)):
                            valido = True
                            dia = dados_da_solicitacao["dia"]
                            horario = dados_da_solicitacao["horario"]
                            leito = quarto["id"]
                            quarto["Quarta"] += 1
                            break
                        else: 
                            if ((dados_da_solicitacao["dia"] == "Quinta") and (quarto["Quinta"] == 0)):
                                valido = True
                                dia = dados_da_solicitacao["dia"]
                                horario = dados_da_solicitacao["horario"]
                                leito = quarto["id"]
                                quarto["Quinta"] += 1
                                break
                            else:
                                if ((dados_da_solicitacao["dia"] == "Sexta") and (quarto["Sexta"] == 0)):
                                    valido = True
                                    dia = dados_da_solicitacao["dia"]
                                    horario = dados_da_solicitacao["horario"]
                                    leito = quarto["id"]
                                    quarto["Sexta"] += 1
                                    break
                                else: 
                                    if ((dados_da_solicitacao["dia"] == "Sabado") and (quarto["Sabado"] == 0)):
                                        valido = True
                                        dia = dados_da_solicitacao["dia"]
                                        horario = dados_da_solicitacao["horario"]
                                        leito = quarto["id"]
                                        quarto["Sabado"] += 1
                                        break

        arquivo_quartos.close()

        return valido, dia, horario, leito

def executar():
    global deslocamento

    consumidor_de_solicitacao = KafkaConsumer(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1),
        auto_offset_reset = "earliest",
        consumer_timeout_ms = 1000
    )
    topico = TopicPartition(PROCESSO_HORARIO, 0)
    consumidor_de_solicitacao.assign([topico])
    consumidor_de_solicitacao.seek(topico, deslocamento)

    for solicitacao in consumidor_de_solicitacao:
        deslocamento = solicitacao.offset + 1

        dados_da_solicitacao = solicitacao.value
        dados_da_solicitacao = json.loads(dados_da_solicitacao)

        valido, dia, horario, leito  = checar_quartos(dados_da_solicitacao)
        if valido:
            dados_da_solicitacao["sucesso"] = 1
            dados_da_solicitacao["dia"] = dia
            dados_da_solicitacao["horario"] = horario
            dados_da_solicitacao["quarto"] = leito
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
