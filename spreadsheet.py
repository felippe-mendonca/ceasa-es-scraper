import gspread
from datetime import datetime

from ceasa import CeasaESMercado


GSPREAD_KEY = "1_z2hxlBVSORUmgIpnWNLqm-g8DrsbxOtc6rY4XodTI8"
GSPREAD_WORKSHEET = "boletins"


class CeasaESSpreadsheet:
    def __init__(self):
        self.gc = gspread.service_account()
        self.sh = self.gc.open_by_key(GSPREAD_KEY)
        self.worksheet = self.sh.worksheet(GSPREAD_WORKSHEET)

    def get_boletins(self):
        r = self.worksheet.get("A2:C")
        mercado_datas = set(map(tuple, r))

        def parse_mercado_data(md):
            mercado_nome, mercado_id, data_str = md
            data = datetime.strptime(data_str, "%d/%m/%Y")
            return (CeasaESMercado(mercado_nome, mercado_id), data)

        return list(map(parse_mercado_data, mercado_datas))

    def add_boletim(self, boletim):
        values = boletim.to_matrix()
        self.worksheet.append_rows(values)

    def add_boletins(self, boletins):
        values = []
        for boletim in boletins:
            values.extend(boletim.to_matrix())
        self.worksheet.append_rows(values)
