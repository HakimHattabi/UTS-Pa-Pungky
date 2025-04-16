import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime

class DatabaseZakat:
    def __init__(self):
        """Inisialisasi koneksi database dan buat tabel jika belum ada"""
        self.connection = None
        self.cursor = None
        
        try:
            # Pertama, connect tanpa database tertentu
            self.connection = mysql.connector.connect(
                host='localhost',
                user='root',    # ganti dengan username MySQL Anda
                password='',     # ganti dengan password MySQL Anda
            )
            
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print("Berhasil terhubung ke MySQL server")
                
                # Buat database jika belum ada
                self.cursor.execute("CREATE DATABASE IF NOT EXISTS db_zakat")
                self.cursor.execute("USE db_zakat")
                
                # Buat tabel pembayar_zakat jika belum ada
                self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS pembayar_zakat (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    nama VARCHAR(100) NOT NULL,
                    alamat VARCHAR(255),
                    telepon VARCHAR(20),
                    jenis_zakat VARCHAR(50) NOT NULL,
                    jumlah_zakat DECIMAL(15,2) NOT NULL,
                    tanggal_bayar DATE NOT NULL,
                    metodo_pembayaran VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                print("Database db_zakat dan tabel pembayar_zakat siap digunakan")
                
        except Error as e:
            print(f"Error saat menghubungkan ke database: {e}")
            # Pastikan semua sumber daya dibersihkan jika gagal
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
            if hasattr(self, 'connection') and self.connection and self.connection.is_connected():
                self.connection.close()
            raise  # Re-raise exception setelah cleanup
    
    def __del__(self):
        """Tutup koneksi database ketika objek dihapus"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection and self.connection.is_connected():
            self.connection.close()
            print("Koneksi MySQL ditutup")
    
    def _check_connection(self):
        """Validasi koneksi dan cursor tersedia"""
        if not self.connection or not self.connection.is_connected() or not self.cursor:
            print("Error: Tidak terhubung ke database")
            return False
        return True
    
    def tambah_pembayaran(self, data):
        """Tambahkan data pembayaran zakat baru"""
        if not self._check_connection():
            return None
            
        try:
            query = """
            INSERT INTO pembayar_zakat 
            (nama, alamat, telepon, jenis_zakat, jumlah_zakat, tanggal_bayar, metodo_pembayaran, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.execute(query, data)
            self.connection.commit()
            print("Data pembayaran zakat berhasil ditambahkan")
            return self.cursor.lastrowid
        except Error as e:
            print(f"Error saat menambahkan data: {e}")
            return None
    
    def tampilkan_data(self, as_dataframe=True):
        """Tampilkan semua data pembayaran zakat"""
        if not self._check_connection():
            return None
            
        try:
            self.cursor.execute("SELECT * FROM pembayar_zakat")
            result = self.cursor.fetchall()
            
            if as_dataframe:
                # Konversi ke pandas DataFrame
                columns = [i[0] for i in self.cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
            else:
                return result
        except Error as e:
            print(f"Error saat mengambil data: {e}")
            return None
    
    def update_status(self, id_pembayaran, status_baru):
        """Update status pembayaran zakat"""
        if not self._check_connection():
            return False
            
        try:
            query = "UPDATE pembayar_zakat SET status = %s WHERE id = %s"
            self.cursor.execute(query, (status_baru, id_pembayaran))
            self.connection.commit()
            print(f"Status pembayaran ID {id_pembayaran} berhasil diupdate menjadi '{status_baru}'")
            return True
        except Error as e:
            print(f"Error saat update status: {e}")
            return False
    
    def hapus_pembayaran(self, id_pembayaran):
        """Hapus data pembayaran zakat"""
        if not self._check_connection():
            return False
            
        try:
            query = "DELETE FROM pembayar_zakat WHERE id = %s"
            self.cursor.execute(query, (id_pembayaran,))
            self.connection.commit()
            print(f"Data pembayaran ID {id_pembayaran} berhasil dihapus")
            return True
        except Error as e:
            print(f"Error saat menghapus data: {e}")
            return False
    
    def cari_pembayaran(self, keyword, by='nama'):
        """Cari data pembayaran berdasarkan keyword"""
        if not self._check_connection():
            return None
            
        try:
            if by not in ['nama', 'alamat', 'telepon', 'jenis_zakat']:
                print("Pencarian hanya bisa dilakukan berdasarkan: nama, alamat, telepon, atau jenis_zakat")
                return None
            
            query = f"SELECT * FROM pembayar_zakat WHERE {by} LIKE %s"
            self.cursor.execute(query, (f"%{keyword}%",))
            result = self.cursor.fetchall()
            
            if result:
                columns = [i[0] for i in self.cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
            else:
                print("Data tidak ditemukan")
                return None
        except Error as e:
            print(f"Error saat mencari data: {e}")
            return None
    
    def total_zakat(self):
        """Hitung total zakat yang telah dibayarkan"""
        if not self._check_connection():
            return 0
            
        try:
            self.cursor.execute("SELECT SUM(jumlah_zakat) FROM pembayar_zakat WHERE status = 'verified'")
            total = self.cursor.fetchone()[0]
            return total if total else 0
        except Error as e:
            print(f"Error saat menghitung total zakat: {e}")
            return 0
    
    def statistik_zakat(self):
        """Tampilkan statistik zakat per jenis"""
        if not self._check_connection():
            return None
            
        try:
            query = """
            SELECT jenis_zakat, COUNT(*) as jumlah_pembayar, 
                   SUM(jumlah_zakat) as total_zakat
            FROM pembayar_zakat
            WHERE status = 'verified'
            GROUP BY jenis_zakat
            """
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            
            if result:
                columns = [i[0] for i in self.cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
            else:
                print("Belum ada data zakat yang terverifikasi")
                return None
        except Error as e:
            print(f"Error saat mengambil statistik: {e}")
            return None


# Contoh penggunaan
if __name__ == "__main__":
    try:
        db = DatabaseZakat()
        
        while True:
            print("\n=== Sistem Manajemen Pembayaran Zakat ===")
            print("1. Tambah Pembayaran Zakat")
            print("2. Tampilkan Semua Pembayaran")
            print("3. Update Status Pembayaran")
            print("4. Hapus Pembayaran")
            print("5. Cari Pembayaran")
            print("6. Total Zakat Terkumpul")
            print("7. Statistik Zakat")
            print("8. Keluar")
            
            pilihan = input("Masukkan pilihan Anda (1-8): ")
            
            if pilihan == "1":
                print("\nTambah Data Pembayaran Zakat")
                nama = input("Nama Pembayar: ")
                alamat = input("Alamat: ")
                telepon = input("Telepon: ")
                jenis_zakat = input("Jenis Zakat (Fitrah/Maal/Infaq): ")
                jumlah_zakat = float(input("Jumlah Zakat (Rp): "))
                tanggal = input("Tanggal Bayar (YYYY-MM-DD): ")
                metode = input("Metode Pembayaran: ")
                
                data = (nama, alamat, telepon, jenis_zakat, jumlah_zakat, tanggal, metode, 'pending')
                db.tambah_pembayaran(data)
                
            elif pilihan == "2":
                print("\nDaftar Pembayaran Zakat:")
                df = db.tampilkan_data()
                if df is not None:
                    print(df.to_string(index=False))
                
            elif pilihan == "3":
                print("\nUpdate Status Pembayaran")
                id_pembayaran = int(input("ID Pembayaran: "))
                status_baru = input("Status Baru (pending/verified/rejected): ")
                db.update_status(id_pembayaran, status_baru)
                
            elif pilihan == "4":
                print("\nHapus Data Pembayaran")
                id_pembayaran = int(input("ID Pembayaran yang akan dihapus: "))
                db.hapus_pembayaran(id_pembayaran)
                
            elif pilihan == "5":
                print("\nCari Data Pembayaran")
                keyword = input("Masukkan kata kunci: ")
                by = input("Cari berdasarkan (nama/alamat/telepon/jenis_zakat): ")
                result = db.cari_pembayaran(keyword, by)
                if result is not None:
                    print(result.to_string(index=False))
                    
            elif pilihan == "6":
                total = db.total_zakat()
                print(f"\nTotal Zakat Terkumpul (Verified): Rp {total:,.2f}")
                
            elif pilihan == "7":
                print("\nStatistik Zakat:")
                stats = db.statistik_zakat()
                if stats is not None:
                    print(stats.to_string(index=False))
                    
            elif pilihan == "8":
                print("Keluar dari program...")
                break
                
            else:
                print("Pilihan tidak valid. Silakan pilih 1-8.")
                
    except Exception as e:
        print(f"Terjadi error utama: {e}")
    finally:
        # Pastikan koneksi ditutup
        if 'db' in locals():
            del db