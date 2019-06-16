import psycopg2
import subprocess
import os
import git
import re

class Database:
    def __init__(self):
        self.schemas = []
        self.connections = {}
        self.connection_info = {}
        # TODO read config from somewhere
        self.config = {}
        self.config['config_section_prefix'] = 'database'
        self.config['database_branch_prefix'] = 'database'
        self.connection = None

    def init(self, argv):
        if os.path.exists('.git'):
            print("Already a git repository, 'git-db init' should be run in a clean directory")
            return 1
        # TODO: initialize default config
    
    def run(self, key, argv):
        switch = {
            'init': self.init,
            'database': self.database,
            'remote': self.remote,
        }
        functionCall = switch.get(key)
        if functionCall is None:
            print("'" + key + "' is not a git-db function. See 'git-db --help'")
            return
        return functionCall(argv)

    # --------------------------------------------------------------
    # -------------------------- git db remote ---------------------
    # --------------------------------------------------------------

    def remote(self, argv):
        if len(argv) < 1 or argv[0] == '--help':
            print('TODO output: 1 database command: some usage info')
            exit(0)
        switch = {
            'add': self.remote_add,
            'patch': self.remote_patch,
            'apply': self.remote_apply,
        }
        functionCall = switch.get(argv[0])
        if functionCall is None:
            print("TODO output 55 some helpful message")
            return
        return functionCall(argv[1:])

    def remote_add(self, argv):
        if len(argv) < 1 or argv[0] == '--help':
            print('TODO output: 3 database command: some usage info')
            exit(0)
        name = argv[0]
        r = git.Repo()
        branch = r.active_branch
        branch = branch.name
        sectionName = "branch \"%s\"" % branch
        rw = r.config_writer()
        rw.set_value(sectionName, 'database', self.config['config_section_prefix'] + '/' + name)
    
    def remote_patch(self, argv):
        self.patchTarget = 'database/local'
        self.patchData = {
            'new': {},
            'delete': {},
            'update': {}
        }
        fileName = self.getNextPatchName()
        if os.path.exists(fileName):
            print('TODO output: 986544 database command: some usage info')
            exit(0)
        # first look at new files and add them to patchData
        self.addNewFilesToPatch('tables')
        # TODO: work on deleted tables
        self.addDeletedFilesToPatch('tables')
        self.addAlteredFilesToPatch('tables')
        self.pushChangesToPatchFile()

    def remote_apply(self, argv):
        if len(argv) < 1 or argv[0] == '--help':
            print('TODO output: 3567 database command: some usage info')
            exit(0)
        patchName = argv[0]

        name = argv[0]
        url, port, username, password = self.getDatabaseConnectionInfo('local')
        connection = self.connect(url, port, username, password)
        cursor = connection.cursor()

        command = 'BEGIN;\n'
        command += self.getFileContent(patchName) + '\n'
        command += 'COMMIT;\n'

        try:
            cursor.execute(command)
            print ('Patch applied')
        except psycopg2.Error as e:
            print ('Error applying patch\n')
            print ('PGSQL error code: ' + e.pgcode + '\n')
            print ('PGSQL error message: \n\n' + e.pgerror + '\n')
            cursor.execute('ROLLBACK;')
            pass
    
    # --------------------------------------------------------------
    # -------------------------- git db database -------------------
    # --------------------------------------------------------------

    def database(self, argv):
        if len(argv) < 1 or argv[0] == '--help':
            print('TODO output: 1 database command: some usage info')
            exit(0)
        switch = {
            'add': self.database_add,
            'check': self.database_check,
            'pull': self.database_pull
        }
        functionCall = switch.get(argv[0])
        if functionCall is None:
            print("TODO output some helpful message")
            return
        return functionCall(argv[1:])
    
    def database_pull(self, argv):
        if len(argv) < 1 or argv[0] == '--help':
            print('usage: git db database pull <name>')
            exit(0)
        r = git.Repo()
        if len(r.index.diff(None)) != 0 or len(r.untracked_files) != 0:
            print('Your working tree has uncomitted changes.')
            print('Please, commit your changes or stash them before you can switch branches')
            exit(0)
        name = argv[0]
        url, port, username, password = self.getDatabaseConnectionInfo(name)
        connection = self.connect(url, port, username, password)
        cursor = connection.cursor()
        message = '[GIT DB] pulled from remote'
        # check does the target database branch exists
        # if anything in the block returns an exception it means the branch does not exists
        try:
            branchName = self.config['database_branch_prefix'] + '/' + name
            branchHash = subprocess.check_output('git rev-parse --verify --quiet ' \
                 + branchName, shell=True)
            if len(branchHash) > 0:
                print('Pulling to existing branch for database: "' + name + '"')
                os.system('git checkout ' + branchName)
                os.system('git ls-tree --name-only HEAD | xargs rm -r')
        except:
            print('Creating database branch for database: "' + name + '"')
            message = '[GIT DB] initial commit'
            self.createDbBranch(name)

        self.createDbDirectories(cursor)
        self.setDatabaseConnections(name)
        for conn in self.connections:
            schemas =  self.getSchemas(conn)
            self.createSchemaDirectories(conn, schemas)
        
        for conn in self.connections:
            schemas =  self.getSchemas(conn)
            print("\r\n======== Connected to: '" + conn + '" ========')
            for schema in schemas:
                print("Fetching table structure for: '" + schema + "'")
                self.createTabletructure(conn, schema)
        r = git.Repo()
        if len(r.index.diff(None)) == 0 and len(r.untracked_files) == 0:
            print('\n\nNothing to commit')
        else:
            r.git.add('.')
            r.git.commit('-m', message)
    
    def database_add(self, argv):
        # two arguments are needed: name and address of the database
        if len(argv) < 2 or argv[0] == '--help':
            print('TODO output: 1 database command: some usage info')
            exit(0)
        name = argv[0]
        # based on how remotes are stored
        sectionName = self.config['config_section_prefix'] + ' "' + name + '"'
        # save to git config file
        r = git.Repo()
        rw = r.config_writer()
        if len(rw.get_value(sectionName, 'url', '')) > 0:
            print('database "' + argv[0] + '" already exists')
            return 0
        urlArray = str.split(argv[1], ':')
        url = urlArray[0]
        rw.set_value(sectionName, 'url', url)
        if len(urlArray) > 1:
            rw.set_value(sectionName, 'port', urlArray[1])
        if len(argv) > 2:
            rw.set_value(sectionName, 'user', argv[2])
        if len(argv) > 3:
            rw.set_value(sectionName, 'password', argv[3])
        # save buffer and release file handle
        rw.release()
        return 0

    def database_check(self, argv):
        # two arguments are needed: name and address of the database
        if len(argv) < 1 or argv[0] == '--help':
            print('TODO output: 1 database command: some usage info')
            exit(0)
        name = argv[0]
        url, port, username, password = self.getDatabaseConnectionInfo(name)
        connection = self.connect(url, port, username, password)
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        if record:
            print("You are connected to - ", record,"\n")
            return True
        else:
            print('')
            return False

    # --------------------------------------------------------------
    # -------------------------- util functions --------------------
    # --------------------------------------------------------------

    def connect(self, url, port, user, password, database=None):
        try:
            return psycopg2.connect(host=url, 
                port=port,
                user=user,
                password=password,
                database=database)
        except:
            if database is not None:
                print("I am unable to connect to the database '" + database + "'")
            else:
                print("I am unable to connect to the database")
            exit(1)
    
    def getDatabaseConnectionInfo(self, name):
        # based on how remotes are stored
        sectionName = self.config['config_section_prefix'] + ' "' + name + '"'

        r = git.Repo()
        rw = r.config_reader()
        # url is mandatory
        url = rw.get_value(sectionName, 'url', '')
        if len(rw.get_value(sectionName, 'url', '')) == 0:
            print('Database "' + argv[0] + '" does not exists')
            return None
        
        port = rw.get_value(sectionName, 'port', '')
        # default PgSQL port, if not specified directly
        if not port:
            port = '5432'
        
        username = rw.get_value(sectionName, 'user', '')
        if len(username) == 0:
            username = input('username:')
        
        password = rw.get_value(sectionName, 'password', '')
        if len(password) == 0:
            password = input('password:')
        
        self.connection_info = {
            'host': url, 
            'port': port, 
            'username': username, 
            'password': password,
        }
        return url, port, username, password

    def setDatabases(self, cursor):
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        records = cursor.fetchall()
        self.databases = [r[0] for r in records]

    def createDbDirectories(self, cursor):
        self.setDatabases(cursor)
        for s in self.databases:
            if os.path.exists(s):
                print("Schema '" + s + "' already exists")
            else :
                os.makedirs(s)
        return

    def createDbBranch(self, name):
        os.system('git checkout --orphan ' + self.config['database_branch_prefix'] + '/' + name)
        try:
            with open(os.devnull, 'wb') as devnull:
                os.system('git rm -rf .', stdout=devnull, stderr=devnull)
        except:
            pass

    def setDatabaseConnections(self, name):
        host, port, user, password = self.getDatabaseConnectionInfo(name)
        for d in self.databases:
            self.connections[d] = self.connect(host, port, user, password, d)
    
    def getSchemas(self, dbName):
        connection = self.connections[dbName]
        cursor = connection.cursor()
        cursor.execute('''SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'pg_catalog') ''')
        records = cursor.fetchall()
        return [r[0] for r in records]
    
    def getTables(self, dbName, schema):
        connection = self.connections[dbName]
        cursor = connection.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'" % schema)
        records = cursor.fetchall()
        return [r[0] for r in records]

    def createSchemaDirectories(self, connection, schemas):
        for s in schemas:
            if os.path.exists(connection + '/' + s):
                print("Schema '" + s + "' in '" + connection + "' already exists")
            else :
                os.makedirs(connection + '/' + s)
        return

    def createTabletructure(self, conn, schema):
        tables = self.getTables(conn, schema)
        path = "%s/%s/tables" % (conn, schema)
        if not os.path.exists(path):
            os.makedirs(path)
        for t in tables:
            fileName = "%s/%s.sql" % (path, t)
            print('====' + fileName)
            os.system("export PGPASSWORD='%s' && pg_dump --host %s --port %s --schema-only --table %s --user %s --file %s --dbname=%s" % (
                self.connection_info['password'],
                self.connection_info['host'],
                self.connection_info['port'],
                schema + '.' + t,
                self.connection_info['username'],
                fileName,
                conn
            ))
    
    # TODO: dummy for the moment, this will need to be config-driven
    # to apply naming convention and adding patch numbers in a meaningful way
    def getNextPatchName(self):
        if not os.path.exists('./patches'):
            os.makedirs('./patches')
        return 'patches/patch_1.sql'
    
    def getCurrentPatchName(self):
        return 'patches/patch_1.sql'
    
    def addNewFilesToPatch(self, directory):
        r = git.Repo()
        currentCommit = r.commit("local")
        remoteCommit = r.commit(self.patchTarget)
        diffIndex = remoteCommit.diff(currentCommit)

        for newItem in diffIndex.iter_change_type('A'):
            print (newItem.b_path)
            self.patchData['new'][newItem.b_path] = self.getFileContent(newItem.b_path)
    
    def getFileContent(self, filePath):
        s = ''
        with open(filePath, 'r') as f:
            s = f.read()
        return s
    
    def pushChangesToPatchFile(self):
        fileName = self.getNextPatchName()
        with open(fileName, 'a') as f:
            for changeType in ['delete', 'new', 'update']:
                for key, data in self.patchData[changeType].items():
                    f.write('-- ' + key + '\n')
                    f.write(data)
    
    def addAlteredFilesToPatch(self, directory):
        r = git.Repo()
        currentCommit = r.commit("local")
        remoteCommit = r.commit(self.patchTarget)
        diffIndex = remoteCommit.diff(currentCommit)

        for newItem in diffIndex.iter_change_type('M'):
            if directory is 'tables':
                addToPatch = self.checkTableDiff(newItem, newItem.b_path)
                if addToPatch:
                    self.patchData['update'][newItem.b_path] = addToPatch
            else:
                self.patchData['update'][newItem.b_path] = self.getFileContent(newItem.b_path)
        return
    
    def addDeletedFilesToPatch(self, directory):
        r = git.Repo()
        currentCommit = r.commit("local")
        remoteCommit = r.commit(self.patchTarget)
        diffIndex = remoteCommit.diff(currentCommit)

        for removedItem in diffIndex.iter_change_type('D'):
            pathArray = removedItem.a_path.split('/')
            if len(pathArray) > 2 \
                and pathArray[2] == directory \
                and directory == 'tables':

                tableName = pathArray[-3] + '\.' \
                    + pathArray[-1].split('.')[0]
                self.patchData['delete'][removedItem.a_path] = \
                    'DROP TABLE IF EXISTS ' + tableName.replace('\.', '.') + ';\n\n'
        return
    
    def checkTableDiff(self, itemBlob, filePath):
        targetFile = itemBlob.a_blob.data_stream.read().decode('utf-8')
        currentFile = itemBlob.b_blob.data_stream.read().decode('utf-8')
        tableName = filePath.split('/')[-3] + '\.' + filePath.split('/')[-1].split('.')[0]
        # split both files per-command
        currentFileParts = currentFile.split(';')
        targetFileParts = targetFile.split(';')
        # get a diff string
        # diff = ''
        # for updatedItem in diffIndex.iter_change_type('M'):
        #     if updatedItem.b_path == itemBlob.b_path:
        #         diff = updatedItem.diff
        #         break
        
        patch = self.compareTableStructure(currentFileParts, targetFileParts, tableName)
        return patch
        
    def compareTableStructure(self, currentFileParts, targetFileParts, tableName):
        sql = 'ALTER TABLE ' + tableName.replace('\.', '.') + '\n'
        # look for the "create table" part to compare first
        createTableCurrent = None
        createTableTarget  = None
        # store everything that is not 'create table' in these array
        remainingFilePartsCurrent = []
        remainingFilePartsTarget = []

        for el in currentFileParts:
            el = "".join(el.lower().split('\n'))
            reObj = re.search('create\s+table\s*' + tableName + '\s*\((.*)\)', el)
            if reObj:
                createTableCurrent = reObj.group(1)
            else:
                remainingFilePartsCurrent.append(el)
        for el in targetFileParts:
            el = "".join(el.lower().split('\n'))
            reObj = re.search('create\s+table\s*' + tableName + '\s*\((.*)\)', el)
            if reObj:
                createTableTarget = reObj.group(1)
            else:
                remainingFilePartsTarget.append(el)

        # split the create table into column definitions. Strip whitespace for eaier comparison        
        createTableColumnsCurrent = createTableCurrent.split(',')
        createTableColumnsTarget = createTableTarget.split(',')
        checkTableColumnsTarget = [''.join(c.split()) for c in createTableColumnsTarget]
        checkTableColumnsCurrent = [''.join(c.split()) for c in createTableColumnsCurrent]
        isAltered = False
        for c in createTableColumnsCurrent:
            colCheck = ''.join(c.split())
            if c is not '' and colCheck not in checkTableColumnsTarget:
                sql += 'ADD COLUMN IF NOT EXISTS ' + c.lstrip() + ',\n'
                isAltered = True
        
        for c in createTableColumnsTarget:
            colCheck = ''.join(c.split())
            if c is not '' and colCheck not in checkTableColumnsCurrent:
                sql += 'DROP COLUMN IF EXISTS ' + c.lstrip().split()[0] + ',\n'
                isAltered = True

        if isAltered:
            sql = sql.rstrip(',\n') + ';\n\n'
        else:
            sql = ''
        
        # whatever remains in the local file and is not identical to the target file
        # should be considered an alteration and added to patch
        isAltered = False
        remainingFilePartsTargetCheck = [''.join(c.split()) for c in remainingFilePartsTarget]
        for c in remainingFilePartsCurrent:
            if c is not '' and c not in remainingFilePartsTargetCheck:
                isAltered = True
                sql += c + ';\n'

        return sql