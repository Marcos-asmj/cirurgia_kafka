from flask import Flask, jsonify

from kafka import KafkaClient, KafkaProducer
from kafka.errors import KafkaError

import hashlib
import random
import string
import json

servico = Flask(__name__)

PROCESSO = "solicitar"

def iniciar():
    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

@servico.route("/agendar/<int:dia>/<int:horario>", methods=["GET", "POST"])
def solicitar_agendamento(dia, horario):
    resultado = {
        "resultado": "falha",
        "id_solicitacao": ""
    }
    
    ID = "".join(random.choice(string.ascii_letters + string.punctuation)for _ in range(12))
    ID = hashlib.md5(ID.encode("utf-8")).hexdigest()

    produtor = KafkaProducer(
        bootstrap_servers = ["kafka:29092"],
        api_version = (0, 10, 1)
    )

    try:
        solicitar_agendamento = {
            "identificacao": ID,
            "sucesso" : 1,
            "dia": dia,
            "horario": horario
        }
        produtor.send(topic=PROCESSO, value=json.dumps(solicitar_agendamento).encode("utf-8"))
        produtor.flush()

        resultado["resultado"] = "sucesso"
        resultado["id_solicitacao"] = ID

    except KafkaError as erro:
        pass

    produtor.close()

    return json.dumps(resultado).encode("utf-8")

if __name__ == "__main__":
    iniciar()

    servico.run(
        host="0.0.0.0",
        debug=True
    )
