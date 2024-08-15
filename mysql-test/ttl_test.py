import threading
import pymysql
import time

def case_1_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 100, "ASSERT"
            #print(f"ThdA: [{col_a}, {col_b.strftime('%Y-%m-%d %H:%M:%S')}, {col_c}]")
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_1_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200)")
        assert false, "ASSERT"
    except pymysql.err.IntegrityError as e:
        assert e.args[0] == 1062, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    try:
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 100, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_2_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_2_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_3_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_3_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_4_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_4_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_5_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(10)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_5_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_6_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(13)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_6_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_7_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_7_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    cur.close()
    B_succ = True


def case_8_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_8_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_9_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_9_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_10_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_10_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_11_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_11_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_12_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 100)")
        time.sleep(14)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_12_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz values(1, sysdate(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case(num):
    global A_succ, B_succ
    global funcs_thdA, funcs_thdB
    A_succ = False
    B_succ = False


    # 1. Connect
    try:
        A_conn = pymysql.connect(host='127.0.0.1',
                               port=3306,
                               database='test',
                               user='root')
    except Exception as e:
        print(f"Thd A connects to DB failed: {e}")
        exit(-1)
    A_succ = True
    try:
        B_conn = pymysql.connect(host='127.0.0.1',
                               port=3306,
                               database='test',
                               user='root')
    except Exception as e:
        print(f"Thd B connects to DB failed: {e}")
        A_conn.close()
        exit(-1)
    B_succ = True

    # 2. Test
    if (A_succ and B_succ):
        A_succ = False
        B_succ = False
        A_thd = threading.Thread(target=funcs_thdA[num - 1], args=(A_conn,))
        B_thd = threading.Thread(target=funcs_thdB[num - 1], args=(B_conn,))

        A_thd.start()
        B_thd.start()

        A_thd.join()
        B_thd.join()
        if (A_succ and B_succ):
            print(f"case_{num} passed")
        else:
            print(f"case_{num} failed")
        A_conn.close()
        B_conn.close()
    else:
        print(f"case_{num} failed")

funcs_thdA = []
funcs_thdB = []
A_succ = False
B_succ = False
if __name__ == '__main__':

    case_num = 12
    # 1. create database and table
    try:
        conn = pymysql.connect(host='127.0.0.1',
                               port=3306,
                               user='root')
    except Exception as e:
        print(f"Connect to DB failed: {e}")
        exit (-1)
    try:
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS test")
        cur.execute("CREATE DATABASE IF NOT EXISTS test")
        cur.execute("CREATE TABLE test.sz ("
                    "col_a INT, "
                    "col_b DATETIME, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
    except Exception as e:
        print(f"Create DB/TABLE failed: {e}")
        conn.close()
        exit (-1)
    conn.close()

    for i in range (1, case_num + 1):
        func_name = f"case_{i}_thdA"
        funcs_thdA.append(eval(func_name))
        func_name = f"case_{i}_thdB"
        funcs_thdB.append(eval(func_name))

    case(1)
    case(2)
    case(3)
    case(4)
    case(5)
    case(6)
    case(7)
    case(8)
    case(9)
    case(10)
    case(11)
    case(12)

