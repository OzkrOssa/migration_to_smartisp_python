import pandas as pd
# import mysql.connector
from constants import PLANS, ROUTERS


class Migration:
    def __init__(self, saeplus_path, network_path, wisphub_path, config):
        self.saeplus_path = saeplus_path
        self.network_path = network_path
        self.wisphub_path = wisphub_path
        self.config = config

        # self.cnx = mysql.connector.connect(**config)
        # self.cursor = self.cnx.cursor()
        print("migration started")

    def merge_saeplus_wisphub(self):
        print("merging saeplus and wisphub dataframes")
        saeplus = pd.read_excel(self.saeplus_path)
        wisphub = pd.read_excel(self.wisphub_path)
        # TODO: rename columns
        self.merged_s_w = pd.merge(
            saeplus, wisphub, on='Documento', how='left')
        print("saeplus and wisphub merged successfully")

    def merge_network(self):
        print("merging saeplus and network dataframes")
        network = pd.read_excel(self.network_path)

        # # Inalambrico
        # self.merged_s_w = self.merged_s_w.dropna(subset=['Plan Internet'])
        self.merged_s_w['Detalle Suscripcion'] = self.merged_s_w.apply(lambda row: row['Plan Internet'] if pd.notna(row['Plan Internet']) else row['Detalle Suscripcion'], axis=1)

        self.merged = pd.merge(self.merged_s_w, network,
                               on='subscriber', how='left')
        self.merged = self.merged[["subscriber", "Documento", "Fecha Contrato", "Estatus", "Saldo", "Suscripción", "Teléfono", "Grupo Afinidad", "Cliente", "Correo",
            "Detalle Suscripcion", "Plan Internet", "ID Contrato", "address", "mikrotik_information.secret", "mikrotik_information.remote_address", "mikrotik_information.bts"]]
        self.merged.to_excel("merged.xlsx")
        print("saeplus, wisphub and network merged successfully")

    def insert_client(self):
        print("inserting data into clients table")
        query = "INSERT INTO clients (id,name,email,phone,address,photo,dni,password,type_document_identification_id,type_organization_id,municipality_id,type_liability_id,type_regime_id,map_marker_icon,odb_geo_json,odb_geo_json_styles,subscriber_no) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        for _, row in self.merged.iterrows():
            data = (row["subscriber"], row["Cliente"], row["Correo"], row["Teléfono"], row["address"], "0",
                    row["Document"], "0", 3, 1, 337, 7, 1, "{}", "{}", "{}", row["subscriber"])
            self.cursor.execute(query, data)

        self.cnx.commit()
        print("clients table updated successfully")

    def insert_client_service(self):
        print("inserting data into client_services table")
        query = "INSERT INTO client_services (client_id,ip,mac,date_in, plan_id,router_id,status,user_hot,pass_hot,online,geo_json_styles) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        HASH = "PggAa9rRIH7nCIL6UBR0IUYupTJGBIoDKo7zj8gRoajGuS7/3nD0AVcbOIR/P2CYZcKFTY2xnnOZM1MooXOkmg=="

        for _, row in self.merged.iterrows():
            MySQLDate = row["Fecha Contrato"].strftime('%Y-%m-%d')
            if row["Estatus"] == "CORTADO" or row["Estatus"] == "POR CORTAR":
                status = "de"
            elif row["Estatus"] == "RETIRADO" or row["Estatus"] == "ANULADO" or row["Estatus"] == "SUSPENDIDO":
                status = "inc"
            else:
                status = "ac"

            data = (row["subscriber"], row["mikrotik_information.remote_address"], "00:00:00:00:00:00", MySQLDate, PLANS[row["Detalle Suscripcion"]],
                    ROUTERS[row["mikrotik_information.bts"]], status, row["mikrotik_information.secret"], HASH, "ver", "{}")
            self.cursor.execute(query, data)
        self.cnx.commit()
        print("client_services table updated successfully")


if __name__ == '__main__':
    config = {
        'user': 'root',
        'password': 'root',
        'host': 'localhost',
        'database': 'saeplus',
        'raise_on_warnings': True
    }
    migration = Migration('saeplus.xlsx', 'network.xlsx',
                          'wisphub.xlsx', config)
    migration.merge_saeplus_wisphub()
    migration.merge_network()