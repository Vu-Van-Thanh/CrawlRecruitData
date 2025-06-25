from itemadapter import ItemAdapter
import pyodbc

class ImportToSQLServer:
    
    def __init__(self):
        #self.conn = mysql.connector.connect(
        #host = '103.56.158.31',
        #port = '3306',
        #  password = 'sinhvienBK',
        #   database = 'ThongTinTuyenDung'
        #)
        self.conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=THISPC\\THANHVU;'
            'DATABASE=RecruitData;'
            'Integrated Security=SSPI;'
            'Connect Timeout=30;'
            'Encrypt=yes;'
            'TrustServerCertificate=yes;'
            'ApplicationIntent=ReadWrite;'
            'MultiSubnetFailover=no'
        )
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        sql = """
            INSERT IGNORE INTO Stg_Data_Raw(Web, Nganh, Link, TenCV, CongTy, TinhThanh, Luong, LoaiHinh, KinhNghiem, CapBac, HanNopCV, YeuCau, MoTa, PhucLoi, SoLuong, Image) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.cur.execute(sql, (item['Web'], item['Nganh'], item['Link'], item['TenCV'], item['CongTy'], item['TinhThanh'], item['Luong'], item['LoaiHinh'], item['KinhNghiem'], item['CapBac'], item['HanNopCV'], item['YeuCau'], item['MoTa'], item['PhucLoi'], item["SoLuong"], item["Img"]))
        self.conn.commit()

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
        
class DatabaseConnector:
    def __init__(self):
        self.connection_string = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=THISPC\\THANHVU;'
            'DATABASE=RecruitData;'
            'Integrated Security=SSPI;'
            'Connect Timeout=30;'
            'Encrypt=yes;'
            'TrustServerCertificate=yes;'
            'ApplicationIntent=ReadWrite;'
            'MultiSubnetFailover=no'
        )

    def connect(self):
        return pyodbc.connect(self.connection_string)

    def get_links_from_database(self):
        connection = self.connect()
        cursor = connection.cursor()

        query = "SELECT Link FROM Stg_Data_Raw WHERE Web = 'TopCV'"
        cursor.execute(query)

        links = [row[0] for row in cursor.fetchall()]

        cursor.close()
        connection.close()

        return links
