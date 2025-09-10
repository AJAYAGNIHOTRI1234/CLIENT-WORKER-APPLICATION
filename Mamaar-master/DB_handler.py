import mysql.connector
from mysql.connector import Error


class DBHandler:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.db = None
        self.cursor = None

    def connection(self):
        try:
            self.db = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.db.cursor(dictionary=True)
        except Error as e:
            raise Exception(f"DB Connection Failed: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()

    # ---------------------- AUTH ---------------------- #
    def validation(self, status, email, password):
        self.connection()
        try:
            if status == "As Client":
                self.cursor.execute(
                    "SELECT user_id FROM client WHERE email = %s AND password = %s",
                    (email, password)
                )
            elif status == "As Worker":
                self.cursor.execute(
                    "SELECT worker_id FROM worker WHERE email = %s AND password = %s",
                    (email, password)
                )
            result = self.cursor.fetchone()
            return result if result else False
        finally:
            self.close()

    def isAdmin(self, email):
        self.connection()
        try:
            self.cursor.execute(
                "SELECT * FROM client WHERE email = %s AND isAdmin = %s",
                (email, 1)
            )
            return bool(self.cursor.fetchone())
        finally:
            self.close()

    # ---------------------- CLIENT ---------------------- #
    def insertClient(self, name, mobile, city, email, password):
        self.connection()
        query = """
            INSERT INTO client(name, mobile, city, email, password)
            VALUES (%s, %s, %s, %s, %s)
        """
        try:
            self.cursor.execute(query, (name, mobile, city, email, password))
            self.db.commit()
            return True
        except Error as e:
            print("Insert client failed:", e)
            return False
        finally:
            self.close()

    def updateClient(self, cid, name, mobile, city, email, password):
        self.connection()
        query = """
            UPDATE client
            SET name = %s, mobile = %s, city = %s, email = %s, password = %s
            WHERE user_id = %s
        """
        try:
            self.cursor.execute(query, (name, mobile, city, email, password, cid))
            self.db.commit()
            return True
        except Error as e:
            print("Update client failed:", e)
            return False
        finally:
            self.close()

    def isClinetExist(self, email):
        self.connection()
        try:
            self.cursor.execute("SELECT * FROM client WHERE email = %s", (email,))
            return bool(self.cursor.fetchone())
        finally:
            self.close()

    def getClientInfo(self, nemail):
        self.connection()
        try:
            self.cursor.execute("SELECT * FROM client WHERE email = %s", (nemail,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def getClientId(self, email):
        self.connection()
        try:
            self.cursor.execute("SELECT user_id FROM client WHERE email = %s", (email,))
            data = self.cursor.fetchone()
            return data["user_id"] if data else None
        finally:
            self.close()

    # ---------------------- WORKER ---------------------- #
    def insertWorker(self, name, mobile, title, city, email, password):
        self.connection()
        query = """
            INSERT INTO worker(name, mobile, title, city, email, password)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            self.cursor.execute(query, (name, mobile, title, city, email, password))
            self.db.commit()
            return True
        except Error as e:
            print("Insert worker failed:", e)
            return False
        finally:
            self.close()

    def isWorkerExist(self, email):
        self.connection()
        try:
            self.cursor.execute("SELECT * FROM worker WHERE email = %s", (email,))
            return bool(self.cursor.fetchone())
        finally:
            self.close()

    def getWorkerInfo(self, nemail):
        self.connection()
        try:
            self.cursor.execute("SELECT * FROM worker WHERE email = %s", (nemail,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def getWorkerId(self, email):
        self.connection()
        try:
            self.cursor.execute("SELECT worker_id FROM worker WHERE email = %s", (email,))
            data = self.cursor.fetchone()
            return data["worker_id"] if data else None
        finally:
            self.close()

    # ---------------------- JOBS ---------------------- #
    def getjobs(self):
        self.connection()
        try:
            self.cursor.execute("""
                SELECT name, w.worker_id, job_id, email, mobile, job_title, city, rating, rate
                FROM worker w, job j
                WHERE w.worker_id = j.worker_id
            """)
            return self.cursor.fetchall()
        finally:
            self.close()

    def getSearchedjobs(self, searchText):
        self.connection()
        try:
            search = f"%{searchText.upper()}%"
            self.cursor.execute("""
                SELECT name, w.worker_id, job_id, email, mobile, job_title, city, rating, rate
                FROM worker w, job j
                WHERE w.worker_id = j.worker_id
                  AND UPPER(j.job_title) LIKE %s
            """, (search,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def getJobDetails(self, job_id):
        self.connection()
        try:
            self.cursor.execute("""
                SELECT name, w.worker_id, job_id, email, mobile, job_title, city, description, rating, rate
                FROM worker w, job j
                WHERE w.worker_id = j.worker_id
                  AND j.job_id = %s
            """, (job_id,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def insertNewJob(self, wid, title, rate, desc):
        self.connection()
        query = "INSERT INTO job(worker_id, job_title, rate, description) VALUES (%s, %s, %s, %s)"
        try:
            self.cursor.execute(query, (wid, title, rate, desc))
            self.db.commit()
            return True
        except Error as e:
            print("Insert job failed:", e)
            return False
        finally:
            self.close()

    def deletejobP(self, job_id):
        self.connection()
        query = "DELETE FROM job WHERE job_id = %s"
        try:
            self.cursor.execute(query, (job_id,))
            self.db.commit()
            return True
        except Error as e:
            print("Delete job failed:", e)
            return False
        finally:
            self.close()

    # ---------------------- REQUESTS ---------------------- #
    def sendRequest(self, jid, wid, cid):
        self.connection()
        query = "INSERT INTO requested(job_id, worker_id, client_id) VALUES (%s, %s, %s)"
        try:
            self.cursor.execute(query, (jid, wid, cid))
            self.db.commit()
            return True
        except Error as e:
            print("Send request failed:", e)
            return False
        finally:
            self.close()

    def cancelRequest(self, worker_id, job_id, client_id):
        self.connection()
        query = "DELETE FROM requested WHERE job_id = %s AND worker_id = %s AND client_id = %s"
        try:
            self.cursor.execute(query, (job_id, worker_id, client_id))
            self.db.commit()
            return True
        except Error as e:
            print("Cancel request failed:", e)
            return False
        finally:
            self.close()

    def acceptRequest(self, worker_id, job_id, client_id):
        self.connection()
        query = "INSERT INTO accepted(job_id, worker_id, client_id) VALUES (%s, %s, %s)"
        try:
            self.cursor.execute(query, (job_id, worker_id, client_id))
            self.db.commit()
            return True
        except Error as e:
            print("Accept request failed:", e)
            return False
        finally:
            self.close()

    def jobClose(self, worker_id, job_id, client_id, ratings):
        self.connection()
        try:
            self.cursor.execute("SELECT rating FROM worker WHERE worker_id = %s", (worker_id,))
            starRate = self.cursor.fetchone()["rating"]

            finalRate = (int(starRate) + int(ratings)) / 2
            self.cursor.execute(
                "UPDATE worker SET rating = %s WHERE worker_id = %s",
                (finalRate, worker_id)
            )
            self.db.commit()

            self.cursor.execute(
                "DELETE FROM accepted WHERE job_id = %s AND worker_id = %s AND client_id = %s",
                (job_id, worker_id, client_id)
            )
            self.db.commit()
            return True
        except Error as e:
            print("Job close failed:", e)
            return False
        finally:
            self.close()

    # ---------------------- CLIENT/WORKER JOB LISTS ---------------------- #
    def getRequestedJobs(self, cid):
        self.connection()
        try:
            self.cursor.execute("""
                SELECT w.name, r.client_id, w.worker_id, j.job_id, w.email, w.mobile, job_title, w.city, rating, rate
                FROM worker w, job j, requested r
                WHERE w.worker_id = r.worker_id
                  AND j.job_id = r.job_id
                  AND r.client_id = %s
            """, (cid,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def getConfirmJobs(self, cid):
        self.connection()
        try:
            self.cursor.execute("""
                SELECT w.name, w.email, r.client_id, w.worker_id, j.job_id, w.mobile, job_title, w.city, rating, rate
                FROM worker w, job j, accepted r
                WHERE w.worker_id = r.worker_id
                  AND j.job_id = r.job_id
                  AND r.client_id = %s
            """, (cid,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def checkRequestedJobs(self, wid):
        self.connection()
        try:
            self.cursor.execute("""
                SELECT c.name, c.user_id, c.city, r.worker_id, j.job_id, c.email, c.mobile, job_title
                FROM client c, job j, requested r
                WHERE c.user_id = r.client_id
                  AND j.job_id = r.job_id
                  AND r.worker_id = %s
            """, (wid,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def checkMyJobs(self, wid):
        self.connection()
        try:
            self.cursor.execute("SELECT job_id, job_title, rate, description FROM job WHERE worker_id = %s", (wid,))
            return self.cursor.fetchall()
        finally:
            self.close()

    def checkConfirmJobs(self, wid):
        self.connection()
        try:
            self.cursor.execute("""
                SELECT c.name, c.user_id, r.worker_id, j.job_id, c.email, c.mobile, job_title, c.city
                FROM client c, job j, accepted r
                WHERE c.user_id = r.client_id
                  AND j.job_id = r.job_id
                  AND r.worker_id = %s
            """, (wid,))
            return self.cursor.fetchall()
        finally:
            self.close()
