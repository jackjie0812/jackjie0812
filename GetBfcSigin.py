import datetime
import csv
import pathlib
import sqlite3
import time
from pathlib import *
from binance.client import Client
from apscheduler.schedulers.blocking import BlockingScheduler


class GetSigint:
    def __init__(self, limit=4, multiple_volume=4, price_multiple=1, multiple_amplitude=5, day_volume=7,
                 **kwargs):
        self.cli = Client()
        self.cli.REQUEST_TIMEOUT = 1000
        self.limit = limit
        self.multiple_volume = multiple_volume
        self.price_multiple = price_multiple
        self.day_volume = day_volume
        self.interval = '1h'
        self.multiple_amplitude = multiple_amplitude

    def _now(self):
        """打印当前时间"""
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return dt

    def ts_convert_datatime(self, unix_timestamp):
        ts = (unix_timestamp / 1000) + 28800  # unix_time_stamp是毫秒，东八区加8小时，换算成秒即是28800.
        return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    # 获取symbol的24小时交易额。
    def get_day_volume(self, symbol):
        return self.cli.futures_ticker(symbol=symbol)  #

    def _check_status(self):
        def file_symbols_csv_status():
            market_data = self.cli.futures_exchange_info()
            symbols = market_data['symbols']
            f_csv = open('symbols.csv', 'w', newline='')
            f_csv_writer = csv.writer(f_csv)
            for each in symbols:
                pair = each['symbol']
                print(pair)
                if '_' not in pair:
                    f_csv_writer.writerow([pair])
            f_csv.close()

        def db_status():
            db_exists = pathlib.Path('binance.db').exists()
            if not db_exists:
                con = sqlite3.connect('binance.db')
                cur = con.cursor()
                sql_create_b_amplitude = """create table if not exists b_amplitude(pno INTEGER primary key autoincrement,
                                                            symbol text,
                                                            m5 real,
                                                            m15 real,
                                                            h1 real,
                                                            h4 real,
                                                            h12 real,
                                                            d1 real,
                                                            create_date datetime)"""
                sql_create_b_sigint = """create table if not exists b_sigint(pno INTEGER primary key autoincrement,
                                                                                  symbol text,
                                                                                  typed text,
                                                                                  interva text,
                                                                                  stance_time text,
                                                                                  hprice real,
                                                                                  curr_price real,
                                                                                  mul_price real,
                                                                                  mul_volume real,
                                                                                  volume_24 real,
                                                                                  is_buy integer,
                                                                                  is_sell integer,
                                                                                  mark_level integer,
                                                                                  create_date datetime)"""
                try:
                    cur.execute(sql_create_b_amplitude)
                    cur.execute(sql_create_b_sigint)
                    print('创建成功..')
                except Exception as e:
                    print(e)
                    print('创建失败')
                finally:
                    cur.close()
                    con.close()

        db_status()
        file_csv_future_symbols = str(Path(Path.cwd()))
        file_csv_future_symbols = file_csv_future_symbols + '/symbols.csv'
        file_csv_future_symbols_is_exists = Path(file_csv_future_symbols).exists()
        if not file_csv_future_symbols_is_exists:
            print(f'{self._now()},{file_csv_future_symbols} status is {file_csv_future_symbols_is_exists},will be '
                  f'DOWNLOAD!!')
            file_symbols_csv_status()
            time.sleep(1)

    def _get_symbols_1500_amplitude_info(self, ):
        print(f'{self._now()} 正在下载symbols的interval数据!')
        interval_list = ['5m', '15m', '1h', '4h', '12h', '1d']
        f = open('symbols.csv')
        symbols = csv.reader(f)
        for symbol in symbols:
            symbol = symbol[0]
            time_frame = [symbol]
            for inter in interval_list:
                data = self._get_symbol_last_candle(symbol, inter, limit=1500)
                count = 0
                suma = 0
                for j in data:
                    result_amp = (float(j[2]) - float(j[3])) / float(j[1]) * 100  # 振幅
                    if result_amp > 0.3:
                        suma += 1
                        count += result_amp
                if suma == 0:  # 防止除数为0的情况发生
                    suma = 1
                    count = 1
                amp_info = round(float(count / suma), 4)
                time_frame.append(amp_info)
            time_frame.append(self._now())
            print(f'{self._now()} 写入{symbol} amplitude info.')
            self._write2db(102, time_frame)
        f.close()

    def _search_db(self, symbol, style, inter='5m'):
        """
        :param style: 1为 b_amplitude, 2 为 b_sigint.
        :param statement:
        :return:
        """
        con = sqlite3.connect('binance.db')
        cur = con.cursor()
        symbol_inter_amp_info = 'None'
        statement = 'None'

        def returns(sj):
            if sj == '5m':
                inter1 = 'm5'
            elif sj == '15m':
                inter1 = 'm15'
            elif sj == '1h':
                inter1 = 'h1'
            elif sj == '4h':
                inter1 = 'h4'
            elif sj == '12h':
                inter1 = 'h12'
            elif sj == '1d':
                inter1 = 'd1'
            return inter1

        if style == 101:  # 查询表b_amplitude 是否有数据!
            statement = "select pno from b_amplitude"
        elif style == 102:
            s = returns(inter)  # sqlite3 不支持数字开头的表头，调转了下，这里转回去。
            statement = "select " + s + " from b_amplitude where symbol == " + "'" + symbol + "'"
        elif style == 2:
            s = returns(inter)
            statement = "select " + s + " from b_sigint where symbol == " + "'" + symbol + "'"
        try:
            cur.execute(statement)
            symbol_inter_amp_info = cur.fetchone()
            if style == 102:
                symbol_inter_amp_info = symbol_inter_amp_info[0]
        except Exception as e:
            print(e)
        finally:
            cur.close()
            con.close()
        # print(f'{self._now()} {symbol} {inter} amp_info: {symbol_inter_amp_info}')
        return symbol_inter_amp_info

    def _get_candle_sigint(self, symbol, data, inter):
        """寻找天地针"""
        symbol_amp_info = float(self._search_db(symbol, 102, inter))
        result_sigint = []
        for i in range(1, len(data)):
            last_candle = data[i - 1]
            current_candle = data[i]
            curr_price = data[len(data) - 1][3]
            last_volume = float(last_candle[5]) if float(last_candle[5]) != 0 else 1
            current_volume = float(current_candle[5])
            current_gain = ((float(current_candle[4]) - float(current_candle[1])) / float(current_candle[1])) * 100
            multi_vol_result = current_volume / last_volume
            zf = ((float(current_candle[2]) - float(current_candle[3])) / float(current_candle[1])) * 100
            # ======================= td_sigint ===========================

            def td_sigint():
                if zf > symbol_amp_info:
                    gain = (abs(float(current_candle[4]) - float(current_candle[1])) / float(current_candle[1])) * 100
                    rise_volume = current_volume / last_volume
                    if ((gain * self.multiple_amplitude) < zf) and (rise_volume > self.multiple_volume):
                        happen_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(current_candle[0]) / 1000))
                        hprice = data[len(data) - 1][2]
                        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        db_list = [symbol, 'td_candle', inter, happen_time, hprice, curr_price, dt]
                        # self._write2db(201, db_list)
                        result_sigint.append(db_list)
            # ======================= volume and price sigint =========================

            def volume_sigint():
                if multi_vol_result >= self.multiple_volume:
                    datav = self.get_day_volume(symbol)
                    datav = int(float(datav['quoteVolume']) / 1000000)
                    if datav > self.day_volume:  # 过滤24小时交易额低于7百刀的币种。
                        happen_time = self.ts_convert_datatime(int(current_candle[0]))
                        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        vol_result = [symbol, 'volume', inter, happen_time, round(multi_vol_result, 3),
                                      round(current_gain, 3), datav, dt]
                        # self._write2db(202, vol_result)
                        result_sigint.append(vol_result)

            def price_sigint():
                if current_gain >= self.price_multiple or current_gain < -1:
                    happen_time = self.ts_convert_datatime(int(current_candle[0]))
                    dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    pri_result = [symbol, 'mul_price', inter, happen_time, curr_price, round(current_gain, 3), dt]
                    # self._write2db(203, pri_result)
                    result_sigint.append(pri_result)

            def bear_bull_sigint():
                last_gain = ((float(last_candle[4]) - float(last_candle[1])) / float(
                    last_candle[4])) * 100  # 收盘价 - 开盘价 / 收盘价  涨幅
                last_amplitude = ((float(last_candle[2]) - float(last_candle[3])) / float(
                    last_candle[4])) * 100  # 最高价 - 最低 / 收盘价 振幅
                gain = ((float(current_candle[4]) - float(current_candle[1])) / float(current_candle[4])) * 100
                current_amplitude = ((float(current_candle[2]) - float(current_candle[3])) / float(
                    current_candle[4])) * 100
                dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                happenstance = self.ts_convert_datatime(int(current_candle[0]))
                last_happenstance = self.ts_convert_datatime(int(last_candle[0]))

                # ============================ bullish ========================================

                if (last_gain < -1) and (gain > (abs(last_gain) * 0.7)) and \
                        (float(current_candle[4]) > (float(last_candle[1]) * 0.6)):
                    # print(f'ok,current_candle time at {} or {current_candle[0]}')
                    result = [symbol, 'bullish', inter, happenstance, current_candle[3], data[len(data) - 1][4], dt]
                    result_sigint.append(result)
                    # self._write2db(206, result)

                # ============================ bearish ========================================

                if (last_gain > 1) and (gain < (last_gain * -0.7)) and \
                        (float(current_candle[4]) < (float(last_candle[3]) * 0.6)):
                    bear_result = [symbol, 'bearish', inter, happenstance, current_candle[3], data[len(data) - 1][4], dt]
                    result_sigint.append(bear_result)
                    # self._write2db(207, bear_result)

                # ============================ last_price ========================================
                if (last_gain >= 2) or (last_gain <= -2):
                    result = [symbol, 'last_price', inter, last_happenstance, current_candle[3], data[len(data) - 1][4], dt]
                    result_sigint.append(result)
                    # self._write2db(208, bear_result)

                # ============================ current_price ========================================
                if (current_gain >= 2) or (current_gain <= -2):
                    result = [symbol, 'curr_price', inter, happenstance, current_candle[3], data[len(data) - 1][4], dt]
                    result_sigint.append(result)
                    # self._write2db(209, bear_result)

            td_sigint()
            volume_sigint()
            price_sigint()
            bear_bull_sigint()

        return result_sigint if len(result_sigint) != 0 else 'None'

    def _write2db(self, style, db_list):
        """

        :param style: 102 -> 1500_amp_info  201 ->td_sigint 202 -> vol_sigint 203 -> price_sigint
        :param db_list:
        :return:
        """
        bear_bull_volume_price_list = [206, 207, 208, 209]
        con = sqlite3.connect('binance.db')
        cur = con.cursor()
        print(f'{self._now()} db_list is {db_list}')
        statement = 'None'
        if style == 102:  # 1500_amp_info
            statement = 'insert into b_amplitude(symbol,m5,m15,h1,h4,h12,d1,create_date) values (?,?,?,?,?,?,?,?)'
        elif style == 201:  # td_sigint 202
            statement = "insert into b_sigint(symbol,typed,interva,stance_time,hprice,curr_price,create_date) values (?,?,?,?,?,?,?)"
        elif style == 202:  # vol_sigint 203
            statement = "insert into b_sigint(symbol,typed,interva,stance_time,mul_volume,mul_price,volume_24,create_date) values (?,?,?,?,?,?,?,?)"
        elif style == 203:  # price_sigint
            statement = "insert into b_sigint(symbol,typed,interva,stance_time,curr_price,mul_price,create_date) values (?,?,?,?,?,?,?)"
        elif style in bear_bull_volume_price_list:
            statement = "insert into b_sigint(symbol,typed,interva,stance_time,hprice,curr_price,create_date) values (?,?,?,?,?,?,?)"
        elif style == 210:
            pass
        elif style == 211:
            pass
        if statement != 'None':
            try:
                cur.execute(statement, db_list)
                con.commit()
                print(f'{self._now()} insert to db success!!')
            except Exception as e:
                print(e)
                print(f'{self._now()} insert to db failed!')
            finally:
                cur.close()
                con.close()

    def _get_symbol_last_candle(self, symbol, interval, limit=4):
        if limit != 4:
            data = self.cli.futures_continous_klines(pair=symbol, interval=interval, limit=limit,
                                                     contractType='PERPETUAL')
            time.sleep(1)
        else:
            data = self.cli.futures_continous_klines(pair=symbol, interval=interval, limit=self.limit,
                                                     contractType='PERPETUAL')
        # print(data)
        return data

    def middleware(self, interval):
        s = self._main(interval)
        print(f'{self._now()} middleware')
        if s != 'None':
            for each in s:
                if len(each) > 1:
                    print('*' * 20, 'start')
                    for j in each:
                        print(j)
                    print('*' * 20, 'end')
                else:
                    print(each)
            print("#" * 20, f'end {self._now()}', interval, "#" * 20)
        else:
            print(s)

    def _main(self, inter):
        def db_amplitude_has_statement():
            r = self._search_db('etc', 101)
            print(f'{self._now()}表amplitude内容 {r}, has_statement。')
            if r is None:
                self._get_symbols_1500_amplitude_info()

        print(f'{self._now()} 程序开始执行 - main')
        self._check_status()
        db_amplitude_has_statement()

        f = open('symbols.csv')
        symbols = csv.reader(f)

        if symbols is None:
            print(f'{self._now()} 程序出错 - main')
            exit('main failed')
        result_list = []
        for symbol in symbols:
            symbol = symbol[0]  # 从csv读取到的是列表形式。
            data = self._get_symbol_last_candle(symbol, inter)
            result_td_candle = self._get_candle_sigint(symbol, data, inter)
            if result_td_candle != 'None':
                result_list.append(result_td_candle)

        f.close()
        return result_list if len(result_list) != 0 else 'None'

    def start(self):
        aps = BlockingScheduler()
        aps.add_job(self.middleware, trigger='cron', minute='*/5', timezone='Asia/Shanghai', misfire_grace_time=60,
                    kwargs={'interval': '5m'})
        aps.add_job(self.middleware, trigger='cron', minute='*/15', second='35', timezone='Asia/Shanghai',
                    misfire_grace_time=60,
                    kwargs={'interval': '15m'})
        aps.add_job(self.middleware, trigger='cron', hour='*', minute='1', second='12', timezone='Asia/Shanghai',
                    misfire_grace_time=60, kwargs={'interval': '1h'})
        aps.add_job(self.middleware, trigger='cron', hour='*/4', minute='2', second='33', timezone='Asia/Shanghai',
                    misfire_grace_time=60, kwargs={'interval': '4h'})
        aps.add_job(self.middleware, trigger='cron', hour='*/12', minute='4', timezone='Asia/Shanghai',
                    misfire_grace_time=60,
                    kwargs={'interval': '12h'})
        aps.add_job(self.middleware, trigger='cron', day='*', timezone='Asia/Shanghai', misfire_grace_time=60,
                    kwargs={'interval': '1d'})
        aps.start()
