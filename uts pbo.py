import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import re

class DatabaseZakat:
    def __init__(self):
        """Initialize database connection and create tables if they don't exist"""
        self.connection = None
        self.cursor = None
        
        try:
            # First, connect without specifying a database
            self.connection = mysql.connector.connect(
                host='localhost',
                user='root',    # change to your MySQL username
                password='',    # change to your MySQL password
                autocommit=False  # We'll manage transactions manually
            )
            
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print("Successfully connected to MySQL server")
                
                # Create database if it doesn't exist
                self._execute_safe("CREATE DATABASE IF NOT EXISTS db_zakat")
                self._execute_safe("USE db_zakat")
                
                # Create pembayar_zakat table if it doesn't exist
                self._execute_safe("""
                CREATE TABLE IF NOT EXISTS pembayar_zakat (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    nama VARCHAR(100) NOT NULL,
                    alamat VARCHAR(255),
                    telepon VARCHAR(20),
                    jenis_zakat VARCHAR(50) NOT NULL,
                    jumlah_zakat DECIMAL(15,2) NOT NULL,
                    tanggal_bayar DATE NOT NULL,
                    metodo_pembayaran VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'verified', 'rejected')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_nama (nama),
                    INDEX idx_status (status),
                    INDEX idx_jenis_zakat (jenis_zakat)
                )
                """)
                print("Database db_zakat and table pembayar_zakat are ready")
                
        except Error as e:
            print(f"Error connecting to database: {e}")
            self._cleanup_resources()
            raise
    
    def __del__(self):
        """Close database connection when object is destroyed"""
        self._cleanup_resources()
        print("MySQL connection closed" if not self.connection or not self.connection.is_connected() else "")
    
    def _cleanup_resources(self):
        """Clean up database resources"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
            if hasattr(self, 'connection') and self.connection and self.connection.is_connected():
                self.connection.close()
        except Exception as e:
            print(f"Error during resource cleanup: {e}")
    
    def _execute_safe(self, query, params=None):
        """Execute a query with error handling and automatic rollback on failure"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return True
        except Error as e:
            print(f"Database error: {e}")
            if self.connection.is_connected():
                self.connection.rollback()
            return False
    
    def _validate_phone(self, phone):
        """Validate phone number format"""
        return re.match(r'^[\d\s\+\-\(\)]{7,20}$', phone) is not None
    
    def _validate_date(self, date_str):
        """Validate date format (YYYY-MM-DD)"""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def _validate_zakat_type(self, zakat_type):
        """Validate zakat type"""
        return zakat_type.lower() in ['fitrah', 'maal', 'infaq', 'fidyah']
    
    def _validate_amount(self, amount_str):
        """Validate zakat amount"""
        try:
            amount = float(amount_str)
            return amount > 0
        except ValueError:
            return False
    
    def _check_connection(self):
        """Validate that connection and cursor are available"""
        if not self.connection or not self.connection.is_connected() or not self.cursor:
            print("Error: Not connected to database")
            return False
        return True
    
    def tambah_pembayaran(self, data):
        """Add new zakat payment record"""
        if not self._check_connection():
            return None
            
        try:
            # Validate input data
            if len(data) != 8:
                print("Error: Invalid data format")
                return None
                
            nama, alamat, telepon, jenis_zakat, jumlah_zakat, tanggal_bayar, metodo_pembayaran, status = data
            
            if not nama or len(nama) > 100:
                print("Error: Nama must be between 1-100 characters")
                return None
                
            if not self._validate_phone(telepon):
                print("Error: Invalid phone number format")
                return None
                
            if not self._validate_zakat_type(jenis_zakat.lower()):
                print("Error: Zakat type must be Fitrah, Maal, Infaq, or Fidyah")
                return None
                
            if not self._validate_amount(jumlah_zakat):
                print("Error: Amount must be a positive number")
                return None
                
            if not self._validate_date(tanggal_bayar):
                print("Error: Date must be in YYYY-MM-DD format")
                return None
                
            if status not in ['pending', 'verified', 'rejected']:
                print("Error: Invalid status")
                return None
            
            query = """
            INSERT INTO pembayar_zakat 
            (nama, alamat, telepon, jenis_zakat, jumlah_zakat, tanggal_bayar, metodo_pembayaran, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            if not self._execute_safe(query, data):
                return None
                
            self.connection.commit()
            print("Zakat payment record added successfully")
            return self.cursor.lastrowid
            
        except Exception as e:
            print(f"Error adding payment record: {e}")
            if self.connection.is_connected():
                self.connection.rollback()
            return None
    
    def tampilkan_data(self, limit=1000):
        """Display all zakat payment records with optional limit"""
        if not self._check_connection():
            return None
            
        try:
            query = "SELECT * FROM pembayar_zakat ORDER BY tanggal_bayar DESC LIMIT %s"
            if not self._execute_safe(query, (limit,)):
                return None
                
            result = self.cursor.fetchall()
            
            if result:
                columns = [i[0] for i in self.cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
            else:
                print("No payment records found")
                return pd.DataFrame()  # Return empty DataFrame for consistency
                
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None
    
    def update_status(self, id_pembayaran, status_baru):
        """Update payment status"""
        if not self._check_connection():
            return False
            
        try:
            # Validate input
            try:
                id_pembayaran = int(id_pembayaran)
            except ValueError:
                print("Error: ID must be an integer")
                return False
                
            if status_baru not in ['pending', 'verified', 'rejected']:
                print("Error: Status must be pending, verified, or rejected")
                return False
            
            # Check if record exists first
            if not self._execute_safe("SELECT 1 FROM pembayar_zakat WHERE id = %s", (id_pembayaran,)):
                return False
                
            if not self.cursor.fetchone():
                print(f"Error: No record found with ID {id_pembayaran}")
                return False
            
            # Update status
            query = "UPDATE pembayar_zakat SET status = %s WHERE id = %s"
            if not self._execute_safe(query, (status_baru, id_pembayaran)):
                return False
                
            self.connection.commit()
            print(f"Payment status for ID {id_pembayaran} updated to '{status_baru}'")
            return True
            
        except Exception as e:
            print(f"Error updating status: {e}")
            if self.connection.is_connected():
                self.connection.rollback()
            return False
    
    def hapus_pembayaran(self, id_pembayaran):
        """Delete payment record"""
        if not self._check_connection():
            return False
            
        try:
            # Validate input
            try:
                id_pembayaran = int(id_pembayaran)
            except ValueError:
                print("Error: ID must be an integer")
                return False
            
            # Check if record exists first
            if not self._execute_safe("SELECT 1 FROM pembayar_zakat WHERE id = %s", (id_pembayaran,)):
                return False
                
            if not self.cursor.fetchone():
                print(f"Error: No record found with ID {id_pembayaran}")
                return False
            
            # Delete record
            query = "DELETE FROM pembayar_zakat WHERE id = %s"
            if not self._execute_safe(query, (id_pembayaran,)):
                return False
                
            self.connection.commit()
            print(f"Payment record ID {id_pembayaran} deleted successfully")
            return True
            
        except Exception as e:
            print(f"Error deleting record: {e}")
            if self.connection.is_connected():
                self.connection.rollback()
            return False
    
    def cari_pembayaran(self, keyword, by='nama', limit=100):
        """Search payment records"""
        if not self._check_connection():
            return None
            
        try:
            # Validate search parameters
            valid_search_fields = ['nama', 'alamat', 'telepon', 'jenis_zakat', 'id']
            if by not in valid_search_fields:
                print(f"Error: Can only search by {', '.join(valid_search_fields)}")
                return None
                
            if not keyword or len(keyword) < 2:
                print("Error: Search keyword must be at least 2 characters")
                return None
                
            # Special handling for ID search
            if by == 'id':
                try:
                    id_val = int(keyword)
                    query = "SELECT * FROM pembayar_zakat WHERE id = %s LIMIT %s"
                    params = (id_val, limit)
                except ValueError:
                    print("Error: ID must be an integer")
                    return None
            else:
                query = f"SELECT * FROM pembayar_zakat WHERE {by} LIKE %s LIMIT %s"
                params = (f"%{keyword}%", limit)
            
            if not self._execute_safe(query, params):
                return None
                
            result = self.cursor.fetchall()
            
            if result:
                columns = [i[0] for i in self.cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
            else:
                print("No matching records found")
                return pd.DataFrame()  # Return empty DataFrame for consistency
                
        except Exception as e:
            print(f"Error searching records: {e}")
            return None
    
    def total_zakat(self, status='verified'):
        """Calculate total zakat collected"""
        if not self._check_connection():
            return 0.0
            
        try:
            if status not in ['pending', 'verified', 'rejected']:
                print("Error: Invalid status filter")
                return 0.0
                
            query = "SELECT SUM(jumlah_zakat) FROM pembayar_zakat WHERE status = %s"
            if not self._execute_safe(query, (status,)):
                return 0.0
                
            total = self.cursor.fetchone()[0]
            return float(total) if total is not None else 0.0
            
        except Exception as e:
            print(f"Error calculating total zakat: {e}")
            return 0.0
    
    def statistik_zakat(self):
        """Show zakat statistics by type"""
        if not self._check_connection():
            return None
            
        try:
            query = """
            SELECT jenis_zakat, COUNT(*) as jumlah_pembayar, 
                   SUM(jumlah_zakat) as total_zakat,
                   AVG(jumlah_zakat) as rata_rata
            FROM pembayar_zakat
            WHERE status = 'verified'
            GROUP BY jenis_zakat
            ORDER BY total_zakat DESC
            """
            
            if not self._execute_safe(query):
                return None
                
            result = self.cursor.fetchall()
            
            if result:
                columns = [i[0] for i in self.cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
            else:
                print("No verified payment records found")
                return pd.DataFrame(columns=['jenis_zakat', 'jumlah_pembayar', 'total_zakat', 'rata_rata'])
                
        except Exception as e:
            print(f"Error retrieving statistics: {e}")
            return None


def display_menu():
    """Display the main menu"""
    print("\n=== Zakat Payment Management System ===")
    print("1. Add Zakat Payment")
    print("2. View All Payments")
    print("3. Update Payment Status")
    print("4. Delete Payment")
    print("5. Search Payments")
    print("6. View Total Zakat Collected")
    print("7. View Zakat Statistics")
    print("8. Exit")


def get_valid_input(prompt, validation_func, error_msg, max_attempts=3):
    """Get validated user input with retries"""
    attempts = 0
    while attempts < max_attempts:
        user_input = input(prompt).strip()
        if validation_func(user_input):
            return user_input
        print(error_msg)
        attempts += 1
    print(f"Maximum attempts reached. Operation cancelled.")
    return None


def main():
    """Main program loop"""
    try:
        db = DatabaseZakat()
        
        while True:
            display_menu()
            choice = input("Enter your choice (1-8): ").strip()
            
            if choice == "1":
                print("\nAdd Zakat Payment")
                
                nama = get_valid_input(
                    "Payer Name (3-100 characters): ",
                    lambda x: 3 <= len(x) <= 100,
                    "Error: Name must be 3-100 characters"
                )
                if nama is None: continue
                
                alamat = input("Address (optional): ").strip() or None
                
                telepon = get_valid_input(
                    "Phone Number: ",
                    db._validate_phone,
                    "Error: Invalid phone number format (7-20 digits with optional +-() spaces)"
                )
                if telepon is None: continue
                
                jenis_zakat = get_valid_input(
                    "Zakat Type (Fitrah/Maal/Infaq/Fidyah): ",
                    db._validate_zakat_type,
                    "Error: Must be Fitrah, Maal, Infaq, or Fidyah"
                )
                if jenis_zakat is None: continue
                
                jumlah_zakat = get_valid_input(
                    "Amount (positive number): ",
                    db._validate_amount,
                    "Error: Must be a positive number"
                )
                if jumlah_zakat is None: continue
                
                tanggal_bayar = get_valid_input(
                    "Payment Date (YYYY-MM-DD): ",
                    db._validate_date,
                    "Error: Invalid date format (use YYYY-MM-DD)"
                )
                if tanggal_bayar is None: continue
                
                metodo_pembayaran = input("Payment Method (optional): ").strip() or "Unknown"
                
                data = (
                    nama,
                    alamat,
                    telepon,
                    jenis_zakat.capitalize(),
                    float(jumlah_zakat),
                    tanggal_bayar,
                    metodo_pembayaran,
                    'pending'  # Default status
                )
                
                payment_id = db.tambah_pembayaran(data)
                if payment_id:
                    print(f"Payment added successfully with ID: {payment_id}")
                
            elif choice == "2":
                print("\nAll Payment Records:")
                limit = input("Enter maximum records to display (default 1000): ").strip()
                try:
                    limit = int(limit) if limit else 1000
                except ValueError:
                    print("Invalid input. Using default limit of 1000")
                    limit = 1000
                    
                df = db.tampilkan_data(limit)
                if df is not None:
                    if not df.empty:
                        print(df.to_string(index=False))
                    else:
                        print("No payment records found")
                
            elif choice == "3":
                print("\nUpdate Payment Status")
                payment_id = input("Payment ID: ").strip()
                new_status = get_valid_input(
                    "New Status (pending/verified/rejected): ",
                    lambda x: x.lower() in ['pending', 'verified', 'rejected'],
                    "Error: Status must be pending, verified, or rejected"
                )
                if new_status and payment_id:
                    db.update_status(payment_id, new_status.lower())
                
            elif choice == "4":
                print("\nDelete Payment")
                payment_id = input("Payment ID to delete: ").strip()
                if payment_id:
                    confirm = input(f"Are you sure you want to delete payment ID {payment_id}? (y/n): ").lower()
                    if confirm == 'y':
                        db.hapus_pembayaran(payment_id)
                
            elif choice == "5":
                print("\nSearch Payments")
                search_by = get_valid_input(
                    "Search by (nama/alamat/telepon/jenis_zakat/id): ",
                    lambda x: x.lower() in ['nama', 'alamat', 'telepon', 'jenis_zakat', 'id'],
                    "Error: Invalid search field"
                )
                if search_by is None: continue
                
                keyword = input("Search keyword: ").strip()
                if keyword:
                    limit = input("Maximum results (default 100): ").strip()
                    try:
                        limit = int(limit) if limit else 100
                    except ValueError:
                        print("Invalid input. Using default limit of 100")
                        limit = 100
                        
                    results = db.cari_pembayaran(keyword, search_by.lower(), limit)
                    if results is not None:
                        if not results.empty:
                            print(results.to_string(index=False))
                        else:
                            print("No matching records found")
                
            elif choice == "6":
                print("\nTotal Zakat Collected")
                status_filter = input("Filter by status (verified/pending/rejected, default verified): ").strip().lower()
                if status_filter not in ['verified', 'pending', 'rejected']:
                    status_filter = 'verified'
                    
                total = db.total_zakat(status_filter)
                print(f"Total {status_filter} zakat collected: Rp {total:,.2f}")
                
            elif choice == "7":
                print("\nZakat Statistics:")
                stats = db.statistik_zakat()
                if stats is not None:
                    if not stats.empty:
                        print(stats.to_string(index=False))
                    else:
                        print("No statistics available")
                
            elif choice == "8":
                print("Exiting the program...")
                break
                
            else:
                print("Invalid choice. Please enter a number between 1-8.")
                
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if 'db' in locals():
            del db


if __name__ == "__main__":
    main()