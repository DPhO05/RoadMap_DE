# Lời giải mẫu Lab 1.4 bước 3: chỉ hàm mot_cau() khác file mà module tạo ra
import duckdb
D = 'lakehouse/source_sim/data/'
con = duckdb.connect()
con.sql(f"CREATE TABLE payments AS SELECT * FROM '{D}olist_order_payments_dataset.csv'")
con.sql("""CREATE TABLE ids AS SELECT DISTINCT order_id FROM payments
           ORDER BY order_id LIMIT 2000""")
ids = [r[0] for r in con.sql("SELECT order_id FROM ids ORDER BY 1").fetchall()]

def vong_lap():
    kq = {}
    for o in ids:
        kq[o] = tuple(con.execute("""SELECT count(*),
            sum(CAST(payment_value AS DECIMAL(12,2))) FROM payments
            WHERE order_id = ?""", [o]).fetchall()[0])
    return kq

def mot_cau():   # {order_id: (số lần thanh toán, tổng tiền)}, bằng một câu SQL
    return {o: (n, tong) for o, n, tong in con.sql("""
        SELECT order_id, count(*), sum(CAST(payment_value AS DECIMAL(12,2)))
        FROM payments JOIN ids USING (order_id) GROUP BY order_id""").fetchall()}

a, b = vong_lap(), mot_cau()
print('hai cách cho cùng kết quả:', a == b)
print('tổng tiền thanh toán của 2.000 đơn:', sum(t for _, t in b.values()))
print('số đơn có hơn một lần thanh toán:', sum(n > 1 for n, _ in b.values()))
