import ccxt, pathlib
import sqlite3
class Get_bfc_amp:
    def __init__(self):
        self.exchange = ccxt.binance()

        pass
    def get_symbol(self):
        symbols = self.exchange.fapiPublicGetExchangeInfo()
        ubw = []
        for each in symbols['symbols']:
            ubw.append(each['symbol'])
        return ubw

    def get_db_status(self):
        is_exists = pathlib.Path('binance.db').exists()
        if not is_exists:
            con = sqlite3.connect('binance.db')
            cur = con.cursor()
            sql_create_table = """create table if not exists b_amplitude(pno INTEGER primary key autoincrement,
                                                        bname text,
                                                        m5 text,
                                                        m15 text,
                                                        h1 text,
                                                        h4 text,
                                                        h12 text,
                                                        d1 text)"""
            try:
                cur.execute(sql_create_table)
                print('创建成功..')
            except Exception as e:
                print(e)
                print('创建失败')
            finally:
                cur.close()
                con.close()

    def getAmpcoins(self,symbols):
        con = sqlite3.connect('binance.db')
        cur = con.cursor()
        timeframe = [
            {'interval': '5m', 'amplitude': 0.81, 'amplitude_qs': 0.70,'limit': 240, 'volume': 0},
            {'interval': '15m', 'amplitude': 0.70, 'amplitude_qs': 0.70,'limit': 108, 'volume': 0},
            {'interval': '30m', 'amplitude': 0.15, 'amplitude_qs': 0.70,'limit': 55, 'volume': 0},
            {'interval': '1h', 'amplitude': 1.8, 'amplitude_qs': 0.70,'limit': 15, 'volume': 0},
            {'interval': '2h', 'amplitude': 2.6, 'amplitude_qs': 0.70,'limit': 10, 'volume': 0},
            {'interval': '4h', 'amplitude': 2.6, 'amplitude_qs': 0.70,'limit': 38, 'volume': 0},
            {'interval': '6h', 'amplitude': 6.0, 'amplitude_qs': 0.70,'limit': 10, 'volume': 0},
            {'interval': '12h', 'amplitude': 4.15, 'amplitude_qs': 0.70,'limit': 20, 'volume': 0},
            {'interval': '1d', 'amplitude': 8.15, 'amplitude_qs': 0.70,'limit': 7, 'volume': 0}]
        for each in symbols:
            if  "_" in each:                #如果是季度合约就跳过。
                continue
            print(f'inner each print coins is: {each}.')
            dxrlb = []
            db_lb = [each]
            nocheklist = ['30m', '2h', '6h']
            for i in timeframe:
                interval = i['interval']
                if interval in nocheklist:
                    continue
                dfh = self.exchange.fapiPublic_get_continuousklines(
                    {'pair': each, 'contractType': 'PERPETUAL', 'interval': interval, 'limit': 1500})
                counta = 0
                suma = 0
                dxr = {}
                for j in dfh:
                    result_amp = (float(j[2]) - float(j[3])) / float(j[1]) * 100
                    if result_amp > 0.3:
                        suma += 1
                        counta += result_amp
                if suma == 0:                #防止除数为0的情况发生
                    suma = 1
                    counta = 1
                print(f'币种%s 符合的次数是%d,结果之和是：%.4f,它们的平均值是%.3f'%(each,suma,counta,(counta/suma)))
                #i['amplitude'] = round(float(count/sum),4)
                dxr['amplitude'] = round(float(counta/suma),4)
                dxr['interval'] = interval
                dxrlb.append(dxr)
                db_lb.append(round(float(counta/suma),4))
                print(f'现在打印的是列表dxrlb %s和字典dxr %s:'%(dxrlb[-1],dxr))
            yxr = dict()
            yxr['symbol'] = each
            yxr['info'] = dxrlb
            print(yxr)
            with open('allCoinIntervalAmplitude.txt', 'a+') as f:
                f.write(str(yxr)+"\n")
            f.close()
            print(db_lb)
            db_tuple = tuple(db_lb)
            sql_insert_data = 'insert into b_amplitude(bname,m5,m15,h1,h4,h12,d1) values (?,?,?,?,?,?,?)'
            try:
                cur.execute(sql_insert_data,db_tuple)
                print('插入数据成功')
                con.commit()
            except Exception as e:
                print(e)
                print('插入数据失败')
        cur.close()
        con.close()

    def main(self):
        self.get_db_status()
        symbos = self.get_symbol()
        self.getAmpcoins(symbos)