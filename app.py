import re
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass


CEASA_ES_BOLETIM_URL = "http://200.198.51.71/detec/filtro_boletim_es/filtro_boletim_es.php"
CEASA_ES_SELECT_ID_MERCADOS = "id_sc_field_mercado"
CEASA_ES_SELECT_ID_DATAS = "id_sc_field_datas"


@dataclass
class CeasaESMercado:
    id: str
    name: str


class CeasaESScraper:
    def _build_data(
        self,
        nm_form_submit="1",
        nmgp_opcao="recarga",
        nmgp_ancora="0",
        nmgp_params="",
        nmgp_idioma_novo="",
        nmgp_schema_f="",
        nmgp_url_saida="",
        nmgp_num_form="",
        mercado="",
        datas="",
    ):
        return {
            "nm_form_submit": nm_form_submit,
            "nmgp_opcao": nmgp_opcao,
            "nmgp_ancora": nmgp_ancora,
            "nmgp_parms": nmgp_params,
            "nmgp_idioma_novo": nmgp_idioma_novo,
            "nmgp_schema_f": nmgp_schema_f,
            "nmgp_url_saida": nmgp_url_saida,
            "nmgp_num_form": nmgp_num_form,
            "mercado": mercado,
            "datas": datas,
        }

    def _parse_mercados(self, select):
        mercados = []
        for option in select.find_all('option'):
            id = option.get('value')
            if id == '0':
                continue
            name = option.text
            mercados.append(CeasaESMercado(id, name))

        return mercados
        

    def _parse_datas(self, select):
        datas = []
        for option in select.find_all('option'):
            data_option = option.get('value')
            match = re.match(r'(\d+\/\d+\/\d+)\s+', data_option)
            if match is None:
                continue
            data = match.groups()[0]
            datas.append(data)
        
        return datas


    def get_mercados(self):
        req = requests.get(CEASA_ES_BOLETIM_URL)
        soup = BeautifulSoup(req.text, "html.parser")

        mercados = []
        select_mercados = soup.find(id=CEASA_ES_SELECT_ID_MERCADOS)
        if select_mercados is not None:
            mercados = self._parse_mercados(select_mercados)

        return mercados

    def get_datas(self, mercado):
        req = requests.post(CEASA_ES_BOLETIM_URL, self._build_data(mercado=mercado.id))
        soup = BeautifulSoup(req.text, "html.parser")

        datas = []
        select_datas = soup.find(id=CEASA_ES_SELECT_ID_DATAS)
        if select_datas is not None:
            datas = self._parse_datas(select_datas)

        return datas



scraper = CeasaESScraper()
mercados = scraper.get_mercados()
for mercado in mercados:
    print(mercado)
    print(scraper.get_datas(mercado))


