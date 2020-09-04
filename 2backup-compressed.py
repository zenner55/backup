repository = '/home/zenner55/repository'
db_path = repository + '/db/backup.db'
dirs_to_backup = ('/home/zenner55/foldertobackup1', '/home/zenner55/foldertobackup2')

import os, sqlite3, hashlib, datetime, sys, shutil
current = ""; backup_id = sys.argv[1] if len(sys.argv) >= 2 else datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S"); backup_id_full_path = (repository +  '/' + backup_id)
if os.path.exists(backup_id_full_path): print("Backup halted.  Backup id exists:\n" + backup_id_full_path); sys.exit(1)
else: os.mkdir(backup_id_full_path)
connection = sqlite3.connect(db_path); cursor = connection.cursor()
for current_row in cursor.execute("""SELECT backup_id FROM current_info_table WHERE status = 'current'"""): current = current_row[0]; break
else: current = backup_id 
def get_hash(file):
    BUF_SIZE = 65536; h = hashlib.sha512()
    with open(file, 'rb') as f:
        while True:
            chunk = f.read(BUF_SIZE)
            if not chunk: f.close(); break
            h.update(chunk)
    return h.hexdigest() 
def insert_file_path(backup_id, hash, path, size, ctime, mtime):
    cursor.execute("""INSERT INTO hash_table (backup_id, hash, path, size, ctime, mtime) VALUES (?, ?, ?, ?, ?, ?)""", (backup_id, hash, path, size, ctime, mtime)); connection.commit()
def link_or_copy(fullpath, backup_id, hash, path, size, ctime, mtime):
    hash = get_hash(path)
    for hash_in_db in cursor.execute("""SELECT backup_id, path FROM hash_table WHERE hash = ?""", (hash,)):
        os.link((repository + "/" + hash_in_db[0] + hash_in_db[1]), fullpath); insert_file_path(backup_id, hash, path, size, ctime, mtime); break
    else:
        shutil.copy2(path, fullpath); insert_file_path(backup_id, hash, path, size, ctime, mtime)
def dir_walk(dir_to_walk):
    for dirname, dirnames, files in os.walk(dir_to_walk,followlinks = False):
        os.makedirs((backup_id_full_path + dirname), exist_ok=True)
        for name in files:
            path = os.path.join(dirname, name); file_full_path = backup_id_full_path + path; stat_result = os.lstat(path)  
            for current_path in cursor.execute("""SELECT file_id, backup_id, hash, path, size, ctime, mtime FROM hash_table WHERE path = ? and backup_id = ?""", (path, current)):
                if current_path[4] != stat_result.st_size or current_path[5] != stat_result.st_ctime_ns or current_path[6] != stat_result.st_mtime_ns:
                    link_or_copy(file_full_path, backup_id, hash, path, stat_result.st_size, stat_result.st_ctime_ns, stat_result.st_mtime_ns)
                else:
                    os.link((repository + "/" + current_path[1] + path), file_full_path); cursor.execute("""UPDATE hash_table SET backup_id = ? WHERE file_id = ?""", (backup_id, current_path[0])); connection.commit()    
                break         
            else:
                link_or_copy(file_full_path, backup_id, hash, path, stat_result.st_size, stat_result.st_ctime_ns, stat_result.st_mtime_ns)
for dir_to_backup in dirs_to_backup: dir_walk(dir_to_backup)    
cursor.execute("""UPDATE current_info_table SET status = ? WHERE backup_id = ?""", ("previous", current)); cursor.execute("""INSERT INTO current_info_table (status, backup_id) VALUES (?, ?)""", ('current', backup_id))
connection.commit(); cursor.close(); connection.close()
