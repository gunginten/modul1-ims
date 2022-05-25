import pymysql
from datetime import datetime
import time
import json
import json

while (1):
    first_boot = 1
    now = datetime.now()
    try:
        connection_to_bank = 1
        # open connection db toko online
        try:
            connToko = pymysql.connect(host='remotemysql.com', user='p5NtbfRlvi',
                                       password='9iOSdUfHJK', database='p5NtbfRlvi')
            curToko = connToko.cursor()
        except:
            print('Tidak bisa terkoneksi ke TOKO!!!')

        # open connection db bank
        try:
            connBank = pymysql.connect(host='remotemysql.com', user='SNcqvTLp70',
                                       password='f0xwam0KIC', database='SNcqvTLp70')
            curBank = connBank.cursor()
        except:
            print('Tidak bisa terkoneksi ke Bank!!!')
            connection_to_bank =0

        sql_select = "SELECT * FROM tb_transaksi"
        curToko.execute(sql_select)
        result = curToko.fetchall()

        sql_select = "SELECT * FROM tb_integrasi"
        curToko.execute(sql_select)
        integrasi = curToko.fetchall()

        print("transaksi len = %d | integrasi len = %d" % (len(result), len(integrasi)))

        # insert listener
        if(len(result) > len(integrasi)):
            print("-- INSERT DETECTED --")
            for data in result:
                a = 0
                for dataIntegrasi in integrasi:
                    if(data[0] == dataIntegrasi[0]):
                        a=1
                if (a==0):
                    print("-- RUN INSERTED FOR ID = %s at %s" % (data[0], now))
                    val = (data[0], data[1], data[2], data[3], data[4])
                    insert_integrasi_toko = "INSERT INTO tb_integrasi (id_transaksi, no_rekening, tgl_transaksi, total_transaksi, status)" \
                                            "VALUES(%s, %s, %s, %s, %s)"
                    curToko.execute(insert_integrasi_toko, val)
                    connToko.commit()

                    if(connection_to_bank==1):
                        insert_integrasi_bank = "INSERT INTO tb_integrasi (id_transaksi, no_rekening, tgl_transaksi, total_transaksi, status)" \
                                                "VALUES(%s, %s, %s, %s, %s)"
                        curBank.execute(insert_integrasi_bank, val)
                        connBank.commit()

                        insert_transaksi_bank = "INSERT INTO tb_transaksi (id_transaksi, no_rekening, tgl_transaksi, total_transaksi, status)" \
                                                "VALUES(%s, %s, %s, %s, %s)"
                        curBank.execute(insert_transaksi_bank, val)
                        connBank.commit()

        # delete listener
        if(len(result) < len(integrasi)):
            print("-- DELETE DETECTED --")
            for dataIntegrasi in integrasi:
                a=0
                for data in result:
                    if(dataIntegrasi[0] == data[0]):
                        a=1
                if (a==0):
                    print("-- RUN DELETE FOR ID = %s at %s --" % (dataIntegrasi[0], now))

                    # delete row in tb_integrasi in db_toko
                    delete_integrasi_toko = "DELETE FROM tb_integrasi WHERE id_transaksi = '%s'" % (dataIntegrasi[0])
                    curToko.execute(delete_integrasi_toko)
                    connToko.commit()

                    if(connection_to_bank==1):
                        delete_integrasi_bank = "DELETE FROM tb_integrasi WHERE id_transaksi = '%s'" % (dataIntegrasi[0])
                        curBank.execute(delete_integrasi_bank)
                        connBank.commit()


        # update listener
        if (result != integrasi):
            for data in result:
                for dataIntegrasi in integrasi:
                    if(data[0] == dataIntegrasi[0]):
                        if(data != dataIntegrasi):
                            print("-- UPDATE DETECTED --")
                            print("-- RUN UPDATE FOR ID = %s at %s --" % (dataIntegrasi[0], now))
                            val = (data[1], data[2], data[3], data[4], data[0])
                            update_integrasi_toko = "update tb_integrasi set no_rekening = %s, tgl_transaksi= %s," \
                                                    "total_transaksi = %s, status = %s where id_transaksi = %s"
                            curToko.execute(update_integrasi_toko, val)
                            connToko.commit()

                            if(connection_to_bank == 1):
                                update_integrasi_bank = "update tb_integrasi set no_rekening = %s, tgl_transaksi= %s," \
                                                        "total_transaksi = %s, status = %s where id_transaksi = %s"
                                curBank.execute(update_integrasi_bank, val)
                                connBank.commit()

                                update_transaksi_bank = "update tb_transaksi set no_rekening = %s, tgl_transaksi = %s, " \
                                                        "total_transaksi = %s, status = %s where id_transaksi = %s"
                                curBank.execute(update_transaksi_bank, val)
                                connBank.commit()

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    #untuk delay
    time.sleep(1)
