from PyQt5 import QtWidgets, QtCore, QtGui
import pyqtgraph as pg
import sys, os
import traceback
import psutil
import redis, zmq
import time, datetime
import yaml
from PyQt5.QtWidgets import QColorDialog
from PyQt5.QtCore import pyqtSlot, Qt
from PyQt5.QtGui import QColor, QIntValidator, QDoubleValidator, QRegExpValidator
import numpy as np
import pandas as pd
from functools import partial
from operator import itemgetter

from multiprocessing import Process, Value  # 导入multiprocessing模块，然后导入Process这个类

import threading
from time import sleep, ctime

import warnings


warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('mode.chained_assignment', None)


class Config:

    def __init__(self, config_file: str):
        with open(config_file, 'r') as f:
            self.Assets = yaml.load(f, Loader=yaml.FullLoader)

    def GetSetGeo(self, Key, Value):
        self.Assets[Key] = Value


class DataLoader:
    '''
    every threading is for a subscriber
    one channel one threading
    every field is saved in the corresponding field in Data:list of dict
    '''

    def __init__(self, threadName: str, KeyList, host, port, passwd, db, ):
        self.threadName = threadName
        self.flag = True
        self.KeyList = KeyList
        self.host = host
        self.port = port
        self.db = db
        self.passwd = passwd
        self.CurrP = 0
        print(f'{self.threadName} is ready to start!')
        self.conn = redis.Redis(host=self.host, port=self.port, password=self.passwd, charset='gb18030',
                           errors='replace', decode_responses=True, db=self.db)
        self.r = self.conn.pipeline(transaction=False)

    def getData(self):
        for key in self.KeyList:
            self.r.get('KZ:'+key+':LATEST')
        temp = self.r.execute()
        self.CurrP = pd.Series(temp, index=self.KeyList, dtype='float')/10000
        # if (self.CurrP==0).any():
        #     self.CurrP = pd.Series(np.ones(len(self.CurrP)), index=self.KeyList, dtype='float')

    def update(self, KeyList):
        self.KeyList = KeyList

    def stop(self):
        self.r.close()
        self.conn.connection_pool.disconnect()

# class MyETFThread(threading.Thread):
#     def __init__(self, Key:str):
#         super(MyETFThread, self).__init__()
#         self.context = zmq.Context()
#         self.socket = self.context.socket(zmq.SUB)
#         self.Flag = True
#         self.Key = Key
#         self.response = None
#
#     def run(self):
#         self.socket.connect("tcp://168.36.1.181:6666")
#         self.socket.setsockopt(zmq.SUBSCRIBE, ('MDTKS'+self.Key).encode('utf-8'))  # 消息过滤
#
#         while True and self.Flag:
#             try:
#                 self.response = int(self.socket.recv().decode('utf-8').split(',')[2])/10000
#                 # print(self.response)
#             except:
#                 pass
#
#
#     def stop(self):
#         self.Flag = False
#         # sleep(0.5)
#         self.socket.close()
#         self.context.destroy()


class MyETFThread:
    def __init__(self):
        self.r = redis.Redis(host='168.36.1.115', db=0, password='', port=6379)
        self.response = None

    def run(self, key: str):
        self.response = float(self.r.get('KZ:'+key+':PRECLOSE'))/pow(10, 7)
        print(self.response)


class MainUI(QtWidgets.QMainWindow):

    def __init__(self):
        super(MainUI, self).__init__()
        self.config = Config('configuration.yaml')
        self.KeyPattern = list(self.config.Assets.keys())
        self.df_datas = [pd.DataFrame()]*len(self.KeyPattern)
        self.ETFvalue = 0
        self.getListInit()

        # self.windowSize = {}

        self.Loader = DataLoader(host='168.36.1.115', port=6379, KeyList=self.df_datas[0].index, threadName='worker', db=0, passwd='')

        # fn_plot_config = 'plot_config.yaml'
        # loading configurations
        # self.config = Config('Configuration.yaml')  # 配置文件

        # main window create
        self.setWindowTitle('Premium')

        # if 'Position' in self.config.Assets.keys():
        #     self.SetGeoWin(self.config.Assets['Position'])
        # self.mainwindow_widget = QtWidgets.QWidget()
        # self.mainwindow_layout = QtWidgets.QGridLayout()
        self.main_widget = QtWidgets.QWidget()
        self.main_layout = QtWidgets.QGridLayout()
        self.main_widget.setLayout(self.main_layout)
        self.setCentralWidget(self.main_widget)
        self.main_layout.setSpacing(0)

        # self.main_widget.setFixedSize(QtCore.QSize(1150, 243+125))

        self.myETFThread = MyETFThread()

        self.FixedFrame()
        self.Combo_Box_settle()
        self.textBars_settle()

        self.windowSize = dict()
        self.windowSize['5'] = QtCore.QSize(1150, 118)

        self.resize(self.windowSize['5'])


        self.timer_init(self.myETFThread)

        # self.ontime = MyThread(threadName='copper', counts=self.counts, channel='H:', host='168.36.1.181',
        #                        port=6379, passwd='', db=9)
        #
        # self.ontime.start()
        # self.timer_init(self.ontime)

    def getListInit(self):
        r = redis.Redis(host='168.36.1.170', port=6379)
        conn_r = r.pipeline(transaction=False)
        for index, keypattern in enumerate(self.KeyPattern):
            keypattern1 = "IDXLST:"+self.config.Assets[keypattern][0]+":*"
            SList1 = np.array(r.keys(keypattern1), dtype='str')
            keypattern2 = "ETFLST:"+self.KeyPattern[index]+":Components:*"
            SList2 = np.array(r.keys(keypattern2), dtype='str')

            for x in SList1:
                conn_r.hmget(x, ['Weight', 'ClosePx', 'ConstituentName'])
            tt1 = conn_r.execute()

            for x in SList2:
                conn_r.hget(x, 'ComponentShare')
            tt2 = conn_r.execute()


            for i in range(len(SList1)):
                SList1[i] = 'S' + SList1[i].split(":")[2][:6]
            for i in range(len(SList2)):
                SList2[i] = 'S' + SList2[i].split(":")[3][:6]



            self.df_datas[index] = pd.DataFrame(tt1, index=SList1, columns=['Weight', 'ClosePx', 'Name'])
            self.df_datas[index]['Components'] = pd.Series(tt2, index=SList2, dtype='float')
            self.df_datas[index]['Weight'] = self.df_datas[index]['Weight'].astype(dtype='float')
            self.df_datas[index]['ClosePx'] = self.df_datas[index]['ClosePx'].astype(dtype='float')
            # print(self.df_datas[index])
        conn_r.close()
        r.connection_pool.disconnect()

    def SetGeoWin(self, size: tuple):
        self.setGeometry(size[0], size[1], size[2], size[3])

    def FixedFrame(self):
        self.FixedFrame_settle('etfCode', True, 0, 0, 1, 1)
        self.FixedFrame_settle('对应指数', True, 0, 1, 1, 1)
        self.FixedFrame_settle('预测差额', True, 0, 2, 1, 1)
        self.FixedFrame_settle('折合点数', True, 0, 3, 1, 1)
        self.FixedFrame_settle('筛选个数', True, 0, 4, 1, 1)
        self.FixedFrame_settle('有利申购差额', True, 3, 0, 1, 1)
        self.FixedFrame_settle('有利赎回差额', True, 3, 6, 1, 1)
        self.FixedFrame_settle('股票代码', True, 4, 0, 1, 1)
        self.FixedFrame_settle('股票名称', True, 4, 1, 1, 1)
        self.FixedFrame_settle('股数偏差', True, 4, 2, 1, 1)
        self.FixedFrame_settle('盈亏', True, 4, 3, 1, 1)
        self.FixedFrame_settle('股票代码', True, 4, 6, 1, 1)
        self.FixedFrame_settle('股票名称', True, 4, 7, 1, 1)
        self.FixedFrame_settle('股数偏差', True, 4, 8, 1, 1)
        self.FixedFrame_settle('盈亏', True, 4, 9, 1, 1)

    def FixedFrame_settle(self, name: str, ReadOnly: bool, row, column, rowspan, colspan):
        fixedInfo = QtWidgets.QLineEdit()
        fixedInfo.setText(name)
        fixedInfo.setFont(QtGui.QFont('Times New Roman', 12, QtGui.QFont.Bold))
        fixedInfo.setStyleSheet('background-color: #F4A460')
        fixedInfo.setReadOnly(ReadOnly)
        fixedInfo.setAlignment(QtCore.Qt.AlignCenter)
        self.main_layout.addWidget(fixedInfo, row, column, rowspan, colspan)
        return fixedInfo

    def Frame_settle(self, name: str, ReadOnly: bool, row, column, rowspan, colspan):
        fixedInfo = QtWidgets.QLineEdit()
        fixedInfo.setText(name)
        fixedInfo.setFont(QtGui.QFont('Times New Roman', 12, QtGui.QFont.Bold))
        # fixedInfo.setStyleSheet('background-color: #F4A460')
        fixedInfo.setReadOnly(ReadOnly)
        fixedInfo.setAlignment(QtCore.Qt.AlignCenter)
        self.main_layout.addWidget(fixedInfo, row, column, rowspan, colspan)
        return fixedInfo

    def Combo_Box_settle(self):
        self.comboBox_widget = QtWidgets.QComboBox()
        self.comboBox_widget.setEditable(False)
        self.comboBox_widget.setFont(QtGui.QFont('Times New Roman', 12, QtGui.QFont.Bold))
        Items = list(self.KeyPattern)
        self.comboBox_widget.addItems(Items)
        self.comboBox_widget.currentIndexChanged[str].connect(self.UpdateInfo)
        # self.comboBox_widget.currentIndexChanged[int].connect(self.UpdateData)
        self.main_layout.addWidget(self.comboBox_widget, 1, 0, 1, 1)

        # self.countBox_widget = QtWidgets.QLineEdit('5')
        # self.countBox_widget.setFont(QtGui.QFont('Times New Roman', 12, QtGui.QFont.Bold))
        # self.countBox_widget.setValidator(QIntValidator(1,25))
        # self.countBox_widget.textChanged.connect(self.UpdateShowNum)
        # self.main_layout.addWidget(self.countBox_widget, 1, 4, 1, 1)

        self.countBox_widget = QtWidgets.QComboBox()
        self.countBox_widget.setEditable(False)
        self.countBox_widget.setFont(QtGui.QFont('Times New Roman', 12))
        Items = [str(x) for x in np.arange(5, 16, 5)]
        self.countBox_widget.addItems(Items)
        self.countBox_widget.currentIndexChanged[str].connect(self.UpdateShowNum)
        self.main_layout.addWidget(self.countBox_widget, 1, 4, 1, 1)

    def textBars_settle(self):
        self.Box11 = self.Frame_settle('', True, 1, 1, 1, 1)
        self.UpdateInfo(self.comboBox_widget.currentText())
        self.Box12 = self.Frame_settle('', True, 1, 2, 1, 1)
        self.Box13 = self.Frame_settle('', True, 1, 3, 1, 1)
        self.Box31 = self.Frame_settle('', True, 3, 1, 1, 1)
        self.Box37 = self.Frame_settle('', True, 3, 7, 1, 1)

        self.showList = []
        for i in range(int(self.countBox_widget.currentText())):
            temp = []
            for j in range(8):
                if j <= 3:
                    temp.append(self.Frame_settle('', True, i+5, j, 1, 1))
                else:
                    temp.append(self.Frame_settle('', True, i+5, j+2, 1, 1))

            self.showList.append(temp)

    def UpdateInfo(self, etfcode: str):
        self.Box11.setText(self.config.Assets[etfcode][0])
        self.Loader.update(self.df_datas[self.comboBox_widget.currentIndex()].index)
        self.myETFThread.run('I'+self.Box11.text()[:6])
        # print(self.myETFThread.response)

    def UpdateShowNum(self, num: str):
        n = int(num)
        for x in self.showList:
            for y in x:
                y.clear()
                y.hide()
                self.main_layout.removeWidget(y)
                y.deleteLater()


        # self.main_layout.update()

        del self.showList
        self.showList = []
        for i in range(n):
            temp = []
            for j in range(8):
                if j <= 3:
                    temp.append(self.Frame_settle('', True, i + 5, j, 1, 1))
                else:
                    temp.append(self.Frame_settle('', True, i + 5, j + 2, 1, 1))

            self.showList.append(temp)

        try:
            self.resize(self.windowSize[num])
        except KeyError or AttributeError:
            self.windowSize[num] = self.size()

        self.main_widget.hide()
        self.main_widget.setGeometry(QtCore.QRect(self.pos(), self.windowSize[num]))
        self.main_layout.setVerticalSpacing(0)
        self.main_layout.setGeometry(QtCore.QRect(self.pos(), self.windowSize[num]))
        self.main_layout.update()
        self.main_widget.show()


    def timer_init(self, thief: MyETFThread):
        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(lambda: self.on_timer_update_info(thief))
        self.timer.start(1000)

    def on_timer_update_info(self, thief: MyETFThread):
        if not thief.response:
            return
        else:
            self.Loader.getData()
            self.df_datas[self.comboBox_widget.currentIndex()]['CurrP'] = self.Loader.CurrP # 股票现价
            self.df_datas[self.comboBox_widget.currentIndex()]['CurrP'][self.df_datas[self.comboBox_widget.currentIndex()]['CurrP'] == 0.0] = \
                self.df_datas[self.comboBox_widget.currentIndex()]['ClosePx'][self.df_datas[self.comboBox_widget.currentIndex()]['CurrP'] == 0.0]

            self.df_datas[self.comboBox_widget.currentIndex()]['DeltaP'] = \
                self.df_datas[self.comboBox_widget.currentIndex()]['ClosePx']-self.df_datas[self.comboBox_widget.currentIndex()]['CurrP'] # 前收价-现价

            self.df_datas[self.comboBox_widget.currentIndex()]['IndexComponents'] = \
                self.config.Assets[self.comboBox_widget.currentText()][1]*thief.response*self.df_datas[self.comboBox_widget.currentIndex()]['Weight']/self.df_datas[self.comboBox_widget.currentIndex()]['ClosePx']/100
            # 一手总股数*当前价位*权重/股票当前价

            self.df_datas[self.comboBox_widget.currentIndex()]['DeltaS'] = \
                self.df_datas[self.comboBox_widget.currentIndex()]['Components']-self.df_datas[self.comboBox_widget.currentIndex()]['IndexComponents']

            self.df_datas[self.comboBox_widget.currentIndex()]['DeltaS'] = \
                self.df_datas[self.comboBox_widget.currentIndex()]['DeltaS'].apply(round)
            # ETF成分-指数成分
            self.df_datas[self.comboBox_widget.currentIndex()]['Profit'] = \
                self.df_datas[self.comboBox_widget.currentIndex()]['DeltaP']*self.df_datas[self.comboBox_widget.currentIndex()]['DeltaS']
            # 价差*股数差
            temp = self.df_datas[self.comboBox_widget.currentIndex()]['Profit'].sort_values()

            # print(self.Loader.CurrP[1])
            # print(self.df_datas[self.comboBox_widget.currentIndex()]['ClosePx'][1])
            # print(self.df_datas[self.comboBox_widget.currentIndex()]['DeltaP'])
            # print(self.df_datas[self.comboBox_widget.currentIndex()]['IndexComponents'][1])
            # print(self.df_datas[self.comboBox_widget.currentIndex()]['DeltaS'])
            # print(thief.response)
            # print(self.df_datas[self.comboBox_widget.currentIndex()]['Weight'][1])
            # print(temp)

            self.on_timer_update_List(temp)

                # try:
        #     # prefix =
        #     currentTimeSec = int((datetime.datetime.now()-self.currentDate).total_seconds())
        #     for ind, line in enumerate(self.conf_lines):
        #         averageType = self.conf_lines[ind]['average']
        #         keyType = self.conf_lines[ind]['keyType']
        #         keyname = self.conf_lines[ind]['keyname']
        #         type = self.conf_lines[ind]['type']
        #         if not self.data_checkbox_dict[keyname+keyType+type].isChecked():
        #             self.time_list[ind] = []
        #             self.data_list[ind] = []
        #             self.data_time_list[ind] = {}
        #             self.plt_list[ind].setData(self.time_list[ind], self.data_list[ind])600
        #             #
        #             # self.slabel_list[ind].
        #         movedis = 0
        #         width = line['width']
        #         color = line['color']
        #         try:
        #             movlen = int(self.data_move_dict[keyname+keyType+type].text())
        #             scale = int(self.data_scale_dict[keyname+keyType+type].text())
        #         except:
        #             movlen = 0
        #             scale = 0
        #
        #         # key = currentTimeSec
        #
        #         if currentTimeSec < 32400+1800+0:
        #             continue
        #         elif (41400<currentTimeSec<46800) or currentTimeSec>54000:
        #             key = currentTimeSec
        #         else:
        #             if line['type'] == 'Data':
        #                 counts = self.dataKey.value
        #                 counts = int(counts)
        #
        #
        # except:
        #     pass

        # self.Loader.getData()
        # self.df_datas[self.comboBox_widget.currentIndex()]['CurrP'] = self.Loader.CurrP
        # print(self.Loader.CurrP)
        # print(self.df_datas[self.comboBox_widget.currentIndex()])

    def on_timer_update_List(self, temp: pd.Series):

        s = int(self.countBox_widget.currentText())
        for i in range(s):
            if temp[i]<0:
                self.showList[i][0].setText(temp.index[i])
                self.showList[i][1].\
                    setText(self.df_datas[self.comboBox_widget.currentIndex()].loc[temp.index[i], 'Name'].decode('gbk'))
                self.showList[i][2].\
                    setText(str(round(self.df_datas[self.comboBox_widget.currentIndex()].loc[temp.index[i], 'DeltaS'])))
                self.showList[i][3].setText(str(round(temp[i], 2)))
            if temp[-i-1]>0:
                self.showList[i][4].setText(temp.index[-i-1])
                self.showList[i][5]. \
                    setText(self.df_datas[self.comboBox_widget.currentIndex()].loc[temp.index[-i-1], 'Name'].decode('gbk'))
                self.showList[i][6]. \
                    setText(str(round(self.df_datas[self.comboBox_widget.currentIndex()].loc[temp.index[-i-1], 'DeltaS'])))
                self.showList[i][7].setText(str(round(temp[-i-1], 2)))

        self.Box31.setText(str(round(temp[temp < 0].sum(), 2)))
        self.Box37.setText(str(round(temp[temp > 0].sum(), 2)))
        tt = temp.sum()
        self.Box12.setText(str(round(tt, 2)))
        self.Box13.setText(str(round(tt/900, 2)))

    def updateYaml(self):
        with open(self.configFile, 'w') as f:
            yaml.dump(self.config.Assets, f)

    def closeEvent(self, event):
        # pos = self.geometry()
        # pos = (pos.left(), pos.top(), pos.width(), pos.height())
        # self.config.GetSetGeo('Position', pos)
        # print("Terminated")

        # for signal in self.signal:
        #     try:
        #         movlen = float(self.data_move_dict[signal].text())
        #         scale = float(self.data_scale_dict[signal].text())
        #     except:
        #         movlen = 0.0
        #         scale = 0.0
        #     self.config.Assets['signal'][signal]['Move'] = movlen
        #     self.config.Assets['signal'][signal]['Scale'] = scale

        # 存储配置文件
        # self.updateYaml()

        # stop threadings
        try:
            self.ontime.stop()
        except:
            pass


def main():
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
    app = QtWidgets.QApplication(sys.argv)
    gui = MainUI()
    gui.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()

