import psycopg2
import sys
import xlrd


class SynchronizeTables:
    def __init__(self, path='./users_list.xls', db_name='db_create', db_user='postgres', db_password='admin',
                 db_host='localhost', db_port='5432', test_method=True):
        """
        :param path: path for xls
        :param db_name: postgres db name
        :param db_user: postgres db user
        :param db_password: postgres db password
        :param db_host: postgres db host
        :param db_port: postgres db port
        :param test_method: if you want run it several times, you will need to put it on default(True)
        """
        self.path = path
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.exel = xlrd.open_workbook(f"{self.path}")
        # db connect
        try:
            self.connect = psycopg2.connect(database=self.db_name,
                                            user=self.db_user,
                                            password=self.db_password,
                                            host=self.db_host,
                                            port=self.db_port)
        except psycopg2.Error as e:
            print("Unable to connect to the database:", e)
            sys.exit(0)
        self.db = self.connect.cursor()
        self.createTables()
        if test_method:
            self.testMethod()
        self.insertTables()
        self.synchronizeTables()

    def createTables(self):
        # User table create
        try:
            self.db.execute(
                '''
                CREATE TABLE IF NOT EXISTS "User" (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE
            );
                '''

            )
        except psycopg2.Error as e:
            print("Error creating table User:", e)
        # ConnectUser create (it's for origin and goal)
        # is_check field is for efficient queries
        try:
            self.db.execute(
                '''
                CREATE TABLE IF NOT EXISTS "ConnectUser" (
                id SERIAL PRIMARY KEY,
                origin_user INT REFERENCES "User"(id),
                goal_user INT REFERENCES "User"(id),
                is_check BOOLEAN DEFAULT FALSE 
            );
                '''
            )
            self.db.execute('TRUNCATE "ConnectUser" CASCADE;')
        except psycopg2.Error as e:
            print("Error creating table ConnectUser:", e)

    def testMethod(self):
        # for testing several time !
        try:
            self.db.execute("DELETE FROM \"User\";")
        except psycopg2.Error as e:
            print('Error deleting table User:', e)
        try:
            self.db.execute("DELETE FROM \"ConnectUser\";")
        except psycopg2.Error as e:
            print('Error deleting table User:', e)

    def insertTables(self):

        # nobate 1 => Sheet 1
        exelSheet = self.exel.sheet_by_index(0)

        # plus two columns and no duplicate
        users = set(exelSheet.col_values(0, 1) + exelSheet.col_values(1, 1))

        # insert users in db
        for user in users:
            try:
                self.db.execute(f"INSERT INTO \"User\" (name) VALUES ('{user}');")
            except psycopg2.Error as e:
                print('Error in Inserting users:', e)
                break

        # insert users id base on Sheet 1
        for i in range(1, len(exelSheet.col_values(0))):
            rows = exelSheet.row_values(i)

            try:
                self.db.execute('SELECT id FROM \"User\" WHERE name = %s;', (rows[0],))
                origin_user = self.db.fetchone()

                self.db.execute('SELECT id FROM \"User\" WHERE name = %s;', (rows[1],))
                goal_user = self.db.fetchone()

                self.db.execute(
                    '''
                    INSERT INTO "ConnectUser" (origin_user, goal_user)
                    VALUES (%s, %s)
                    ''',
                    (origin_user, goal_user)

                )

            except psycopg2.Error as e:
                print('Error in Select User:', e)
                break

    def synchronizeTables(self):
        # nobate 2 => Sheet 2
        exelSheet_2 = self.exel.sheet_by_index(1)

        for i in range(1, len(exelSheet_2.col_values(0))):
            # first: check is any same and put is_check=True
            rows = exelSheet_2.row_values(i)
            self.db.execute('SELECT id FROM \"User\" WHERE name = %s;', (rows[0],))
            origin_user = self.db.fetchone()[0]

            self.db.execute('SELECT id, name FROM \"User\" WHERE name = %s;', (rows[1],))
            goal_user = self.db.fetchone()[0]

            self.db.execute(
                f'''
                SELECT COUNT(*) FROM "ConnectUser"
                WHERE origin_user = {origin_user} AND goal_user = {goal_user} AND is_check= False
                '''
            )

            # check is there any or not
            if self.db.fetchone()[0] > 0:
                self.db.execute(
                    f'''
                    UPDATE "ConnectUser"
                    SET is_check = TRUE
                    WHERE origin_user = {origin_user} AND goal_user = {goal_user} AND is_check= FALSE
                    '''
                )
            else:
                # second: check equal origin_user and sync the goal_user
                self.db.execute(
                    f'''
                        SELECT COUNT(*) FROM "ConnectUser"
                        WHERE origin_user = {origin_user} AND is_check= FALSE
                        '''
                )

                if self.db.fetchone()[0] > 0:
                    self.db.execute(
                        f"""
                            UPDATE "ConnectUser"
                            SET is_check = TRUE, goal_user = {goal_user}
                            WHERE CTID IN ( SELECT CTID FROM "ConnectUser" WHERE origin_user = {origin_user} AND is_check= FALSE LIMIT 1)
                            """
                    )
                else:
                    # third: check equal goal_user and sync the origin_user
                    self.db.execute(
                        f'''
                        SELECT COUNT(*) FROM "ConnectUser"
                        WHERE goal_user = {goal_user} AND is_check= FALSE
                        '''
                    )

                    if self.db.fetchone()[0] > 0:
                        self.db.execute(
                            f'''
                            UPDATE "ConnectUser"
                            SET is_check = TRUE , origin_user = {origin_user}
                            WHERE CTID IN ( SELECT CTID FROM "ConnectUser" WHERE goal_user = {goal_user} AND is_check= FALSE LIMIT 1)
                            '''
                        )
        # forth: the other is_check = False is 100% removed
        self.db.execute(
            '''
            DELETE FROM "ConnectUser"
            WHERE is_check = FALSE

            '''
        )
        for i in range(1, len(exelSheet_2.col_values(0))):
            # fiftieth: check is there any row_values that is not in db => if not add it (new rows add to Sheet 2)
            rows = exelSheet_2.row_values(i)
            self.db.execute('SELECT id FROM \"User\" WHERE name = %s;', (rows[0],))
            origin_user = self.db.fetchone()[0]

            self.db.execute('SELECT id, name FROM \"User\" WHERE name = %s;', (rows[1],))
            goal_user = self.db.fetchone()[0]
            self.db.execute(
                f'''
                    SELECT * FROM "ConnectUser"
                    WHERE origin_user = {origin_user} AND goal_user = {goal_user} AND is_check= TRUE
                    '''
            )
            result = self.db.fetchone()
            if result is None:
                self.db.execute(
                    '''
                    INSERT INTO "ConnectUser" (origin_user, goal_user, is_check)
                    VALUES (%s, %s, %s)
                    ''',
                    (origin_user, goal_user, True)

                )
        # ready for next xls update
        self.db.execute(
            f'''
                UPDATE "ConnectUser"
                SET is_check = FALSE
            '''
        )

        # commit changes and close
        self.connect.commit()
        self.db.close()
        self.connect.close()


SynchronizeTables()
