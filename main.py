from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests as rq

codigos = []
datas = []
lista = []


def createCodes():
    data = pd.read_excel('./resources/dados.xlsx')

    df = data.fillna(0)

    for index, row in df.iterrows():
        codigos.append(str(row['REGIAO']) + str(row['COD']).zfill(5))


def createDates():
    ano = 2016
    mes = 1

    while ano < 2018 or mes <= 12:
        for index in range(0, 1):
            if mes <= 9:
                str(mes).zfill(2)
            elif mes > 9:
                str(mes)
            if mes > 12:
                mes = 1
                ano += 1

            datas.append(str(ano) + str(mes).zfill(2))

            mes += 1


def convertToTxt():
    text_file = open("Saida.txt", "w")

    for item in lista:
        text_file.write(item + "\n")
    text_file.close()


def loadAPI():
    executor = ThreadPoolExecutor(72)
    # codigo = '2800308'
    listaF = []
    for data in datas:
        for codigo in codigos:
            listaF.append(executor.submit(makeRequest, data, codigo))
    for futures in listaF:
        futures.result()


def makeRequest(data, codigo):
    subscription_key = "YOUR_ACCESS_KEY"
    assert subscription_key

    search_url = 'http://www.transparencia.gov.br/api-de-dados/bolsa-familia-por-municipio/?mesAno=' + data + '&codigoIbge=' + codigo + '&pagina=1 '
    # search_term = "Governo"

    # headers = {"Ocp-Apim-Subscription-Key": subscription_key}
    # params = {"q": search_term, "textDecorations": True, "textFormat": "HTML"}
    # print(str(data) + str("X") + str(codigo))
    # response = rq.get(search_url, headers=headers, params=params)
    response = rq.get(search_url)
    response.raise_for_status()
    search_results = response.json()[0]
    lista.append(
        str(search_results['municipio']['codigoIBGE']) + ';' + str(
            search_results['municipio']['nomeIBGE']) + ';' +
        str(search_results['municipio']['uf']['sigla']) + ';' + str(
            search_results['municipio']['uf']['nome']) + ';' + str(search_results['dataReferencia']) + ';' +
        str(search_results['valor']) + ';' + str(search_results['quantidadeBeneficiados']))
    print(lista)
    convertToTxt()


def main():
    createCodes()
    createDates()
    loadAPI()


if __name__ == '__main__':
    main()
