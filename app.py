import re
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List


CEASA_ES_BOLETIM_HOST = "200.198.51.71"
CEASA_ES_BOLETIM_FILTRO_PATH = "/detec/filtro_boletim_es/filtro_boletim_es.php"
CEASA_ES_BOLETIM_PATH = "/detec/boletim_completo_es/boletim_completo_es.php"
CEASA_ES_SELECT_ID_MERCADOS = "id_sc_field_mercado"
CEASA_ES_SELECT_ID_DATAS = "id_sc_field_datas"


@dataclass
class CeasaESMercado:
    id: str
    name: str


@dataclass
class Produto:
    nome: str
    embalagem: str
    p_min: float
    p_comum: float
    p_max: float
    situacao: str

    def __init__(self, nome, embalagem, p_min, p_comum, p_max, situacao):
        self.nome = nome.strip()
        self.embalagem = embalagem.strip()
        self.p_min = float(p_min.strip().replace(',', '.'))
        self.p_comum = float(p_comum.strip().replace(',', '.'))
        self.p_max = float(p_max.strip().replace(',', '.'))
        self.situacao = situacao.strip()


@dataclass
class CeasaESBoletim: 
    mercado: CeasaESMercado
    data: datetime
    produtos: List[Produto]


class CeasaESScraper:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "pt-BR,pt",
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        )

    def _make_url(self, path):
        return f"http://{CEASA_ES_BOLETIM_HOST}{path}"

    def _build_data(
        self,
        nm_form_submit="1",
        nmgp_opcao="recarga",
        nmgp_ancora="0",
        nmgp_parms="",
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
            "nmgp_parms": nmgp_parms,
            "nmgp_idioma_novo": nmgp_idioma_novo,
            "nmgp_schema_f": nmgp_schema_f,
            "nmgp_url_saida": nmgp_url_saida,
            "nmgp_num_form": nmgp_num_form,
            "mercado": mercado,
            "datas": datas,
        }

    def _make_nmgp_parms(self, mercado, data):
        data_us = data.strftime('%m/%d/%Y')
        return f"mercod?#?{mercado}?@?data?#?{data_us}?@?"

    def _parse_mercados(self, soup):
        mercados = []
        select = soup.find(id=CEASA_ES_SELECT_ID_MERCADOS)
        if select is None:
            return mercados

        for option in select.find_all("option"):
            id = option.get("value")
            if id == "0":
                continue
            name = option.text
            mercados.append(CeasaESMercado(id, name))

        return mercados

    def _parse_datas(self, soup):
        datas = []
        select = soup.find(id=CEASA_ES_SELECT_ID_DATAS)
        if select is None:
            return datas

        for option in select.find_all("option"):
            data_option = option.get("value")
            match = re.match(r"(\d+\/\d+\/\d+)\s+", data_option)
            if match is None:
                continue
            data_str = match.groups()[0]
            data = datetime.strptime(data_str, '%d/%m/%Y')
            datas.append(data)

        return datas
    
    def _parse_produto(self, soup):
        nome = soup.find(id=re.compile(r'id_sc_field_prdnom_\d+'))
        embalagem = soup.find(id=re.compile(r'id_sc_field_embdesresu_\d+'))
        p_min = soup.find(id=re.compile(r'id_sc_field_pboprcmin_\d+'))
        p_comum = soup.find(id=re.compile(r'id_sc_field_pboprccomum_\d+'))
        p_max = soup.find(id=re.compile(r'id_sc_field_pboprcmax_\d+'))
        situacao = soup.find(id=re.compile(r'id_sc_field_mersit_\d+'))

        params = [nome, embalagem, p_min, p_comum, p_max, situacao]
        if any(map(lambda x: x is None, params)):
            return None
        
        params = list(map(lambda x: x.text, params))
        return Produto(*params)

    def _parse_produtos(self, soup):
        produtos = []
        produtos_soup = soup.find_all(id=re.compile(r'SC_ancor.*'))        
        for p_soup in produtos_soup:
            p = self._parse_produto(p_soup)
            if p is not None:
                produtos.append(p)
        
        return produtos

    def get_mercados(self):
        req = self.s.get(self._make_url(CEASA_ES_BOLETIM_FILTRO_PATH))
        soup = BeautifulSoup(req.text, "html.parser")
        return self._parse_mercados(soup)

    def get_datas(self, mercado):
        req = self.s.post(
            self._make_url(CEASA_ES_BOLETIM_FILTRO_PATH),
            self._build_data(mercado=mercado.id),
        )
        soup = BeautifulSoup(req.text, "html.parser")
        return self._parse_datas(soup)

    def get_boletim(self, mercado, data):
        url = self._make_url(CEASA_ES_BOLETIM_PATH)
        nmgp_parms = self._make_nmgp_parms(mercado.id, data)
        req = self.s.post(url , data=self._build_data(nmgp_parms=nmgp_parms))
        soup = BeautifulSoup(req.text, "html.parser")
        produtos = self._parse_produtos(soup)
        return CeasaESBoletim(mercado, data, produtos)


scraper = CeasaESScraper()
mercados = scraper.get_mercados()
for mercado in mercados:
    for data in scraper.get_datas(mercado):
        boletim = scraper.get_boletim(mercado, data)
