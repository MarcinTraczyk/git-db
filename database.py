import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from time import time
import subprocess
import os
import git
import re
import getpass
import shutil

class Database:
    def __init__(self):
        self.schemas = []
        self.connections = {}
        self.connection_info = {}
        
        if os.path.exists('.git'):
            r = git.Repo()
            rw = r.config_reader()
            sectionName = 'git-db'
            self.config = {}
            self.config['config_section_prefix'] = rw.get_value(sectionName, 'configsectionprefix', '')
            self.config['database_branch_prefix'] = rw.get_value(sectionName, 'databasebranchprefix', '')
            self.config['database'] = rw.get_value(sectionName, 'database', '')
            self.config['default_database'] = rw.get_value(sectionName, 'defaultdatabase', '')
            self.config['store_migrations'] = rw.get_value(sectionName, 'storemigrations', '')
            self.config['ignore_db'] = rw.get_value(sectionName, 'ignoredb', '')
            self.config['ignore_schema'] = rw.get_value(sectionName, 'ignoredb', 'git_db')
            rw.release()
        self.connection = None

    def init(self, argv):
        if not os.path.exists('.git'):
            os.system('git init')
        r = git.Repo()
        rw = r.config_writer()
        sectionName = 'git-db'
        rw.set_value(sectionName, 'configsectionprefix', 'database')
        rw.set_value(sectionName, 'databasebranchprefix', 'database')
        rw.set_value(sectionName, 'database', 'pgsql')
        rw.set_value(sectionName, 'storemigrations', 'database')
        rw.release()
    
    def run(self, key, argv):
        switch = {
            'init': self.init,
            'database': self.database,
            'remote': self.remote,
            'patch': self.patch,
            'query': self.query
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
            'add': self.remote_add
        }
        functionCall = switch.get(argv[0])
        if functionCall is None:
            print("TODO output 55 some helpful message")
            return
        return functionCall(argv[1:])

    def remote_add(self, argv):
        if len(argv) < 1 or argv[0] == '--help':
            print('usage: git db remote add <name>')
            exit(0)
        name = argv[0]
        r = git.Repo()
        branch = r.active_branch.name
        sectionName = "branch \"%s\"" % branch
        rw = r.config_writer()
        if not rw.has_section('%s "%s"' % (self.config['config_section_prefix'], name)):
            print("Database '" + name + "' does not exist")
            exit(1)
        rw.set_value(sectionName, 'database', self.config['config_section_prefix'] + '/' + name)
        # TODO: add more patch numbering options
        rw.set_value(sectionName, 'numbering', 'simple')
        rw.set_value(sectionName, 'current', 0)
        rw.release()
        print("Branch '" + branch + "' set to track database '" + name + "'")
    
    def patch(self, argv):
        if len(argv) < 1 or argv[0] == '--help':
            print('TODO output: 1 database command: some usage info')
            exit(0)
        switch = {
            'create': self.patch_create,
            'apply': self.patch_apply
            # 'migration': self.patch_make_migration
        }
        functionCall = switch.get(argv[0])
        if functionCall is None:
            print("TODO output 55 4343 some helpful message")
            return
        return functionCall(argv[1:])

    # --------------------------------------------------------------
    # -------------------------- git db query ----------------------
    # --------------------------------------------------------------

    def query(self, argv):
        r = git.Repo()
        rw = r.config_reader()
        if len(argv) < 1 or argv[0] == '--help':
            print('usage: git db query <database name>')
            exit(0)
        database = argv[0]

        name = rw.get_value('git_db', 'query_name', '{branch}/{timestamp}.sql')
        name, timestamp = self.replaceWildcards(name)

        nameArray = str.split(name, '/')
        subdir = None
        if len(nameArray) > 1:
            name = nameArray[-1]
            subdir = '/'.join(nameArray[:-1])

        queryPath = database + '/queries'
        if subdir is not None:
            queryPath += '/' + subdir
        if not os.path.exists(queryPath):
            os.makedirs(queryPath)
            
        filePath = queryPath + '/' + name
        os.system('touch ' + filePath)
        
        context = {
            'database': database,
            'record': {
                'name': '\'' + name + '\'',
                'path': '\'' + queryPath + '/' + name + '\'',
                'timestamp': 'to_timestamp(' + timestamp + ')',
                'namespace': '\'' + subdir + '\''
            }
        }
        self.setPatchTarget()
        self.registerQuery(context)
        print("new query file was created: '" + filePath + "'")
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
            'pull': self.database_pull,
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

        self.setDatabases(cursor)
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
                self.createTableStructure(conn, schema)
        r = git.Repo()
        if len(r.index.diff(None)) == 0 and len(r.untracked_files) == 0:
            print('\n\nNothing to commit')
        else:
            r.git.add('.')
            r.git.commit('-m', message)
    
    def database_add(self, argv):
        setAsDefault = False
        if '--default' in argv:
            setAsDefault = True
            argv.remove('--default')

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
        if setAsDefault:
            rw.set_value('git-db', 'defaultdatabase', name)
        
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
    # -------------------------- git db patch ----------------------
    # --------------------------------------------------------------
    
    def patch_create(self, argv):
        useNextNumber = True 
        if '--overwrite' in argv:
            useNextNumber = False
            self.deletePatchFiles()
            argv.remove('--overwrite')
        # read patch target (database the patch is for) from branch config
        if len(argv) > 0 and argv[0] != '--help':
            self.patchTarget = argv[0]
        else:
            self.setPatchTarget()
        
        dbName = self.getDatabaseFromPatchTarget()
        url, port, username, password = self.getDatabaseConnectionInfo(dbName)
        connection = self.connect(url, port, username, password)
        cursor = connection.cursor()
        self.setDatabases(cursor)
        self.resetPatchData()
        self.setDatabaseConnections(self.getDatabaseFromPatchTarget())

        # first look at new files and add them to patchData
        self.addNewFilesToPatch('tables')
        self.addDeletedFilesToPatch('tables')
        self.addAlteredFilesToPatch('tables')

        dbNeedsPatch = False
        if self.checkPatchData():
            self.pushChangesToPatchFile(useNextNumber)
            dbNeedsPatch = True
            useNextNumber = False

        # TODO: add more than tables like so:
        # self.addNewFilesToPatch('views')
        # self.addDeletedFilesToPatch('views')
        # self.addAlteredFilesToPatch('views')
        # if self.checkPatchData():
        #     self.pushChangesToPatchFile(useNextNumber)
        #     dbNeedsPatch = True
        #     useNextNumber = False

        self.addQueriesToPatch()
        if self.checkPatchData():
            self.pushChangesToPatchFile(useNextNumber)
            dbNeedsPatch = True
            useNextNumber = False

        if dbNeedsPatch:
            fileName = self.getPatchName(False)
            print("Patch created: " + fileName)
        else:
            print("Nothing to patch")

    def patch_apply(self, argv):
        if len(argv) > 0 and argv[0] == '--help':
            print('usage: git db patch apply <database name> <patch name>')
            # exit(0)
        elif len(argv) == 1:
            connectionName = argv[0]
            patchName = self.getPatchName(False).split('/')[-1]
            self.patchTarget = 'database/' + connectionName
        elif len(argv) == 2:
            connectionName = argv[0]
            patchName = argv[1]
            self.patchTarget = 'database/' + connectionName
        else:
            self.setPatchTarget()
            connectionName = self.getDatabaseFromPatchTarget()
            patchName = self.getPatchName(False).split('/')[-1]
        print('Do you want to apply patch \'%s\' to the database \'%s\'? [y/n]' 
            % (patchName, connectionName))
        choice = input().lower()
        if (choice != 'y'):
            print ('[Abort]')
            exit(0)

        url, port, username, password = self.getDatabaseConnectionInfo('local')
        conn = self.connect(url, port, username, password)
        cursor = conn.cursor()
        self.setDatabases(cursor)
        self.setDatabaseConnections(connectionName)

        for f in os.listdir('patches/'+ patchName):
            ext = f.split('.')[-1]
            name = f.split('.')[0]
            if ext == 'sql' and name not in self.connections.keys():
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE %s;" % name)
                self.setDatabases(cursor)
                self.setDatabaseConnections(connectionName)
                self.registerPatch(patchName, name)
                self.registerExistingFiles(patchName, name)
                
        for dbName, connection in self.connections.items():
            patchFilePath = 'patches/' + patchName + '/' + dbName + '.sql'
            if (os.path.exists(patchFilePath)):
                cursor = connection.cursor()
                print('\n\nApplying patch file \'%s\'' % patchFilePath)
                    
                command = 'BEGIN;\n'
                command += self.getFileContent(patchFilePath) + '\n'
                command += 'COMMIT;\n'

                try:
                    cursor.execute(command)
                    cursor.execute('''UPDATE git_db.patch 
                        SET applied = TRUE, applied_timestamp = current_timestamp
                        WHERE name = %s;

                        UPDATE git_db.query 
                        SET applied = TRUE, applied_timestamp = current_timestamp
                        WHERE applied_patch_id = (
                            SELECT id FROM git_db.patch WHERE name = %s
                        );''', (patchName, patchName))
                    connection.commit()
                    print ('...ok')
                except psycopg2.Error as e:
                    print ('Error applying patch')
                    print ('PGSQL error code: ' + e.pgcode)
                    print ('PGSQL error message:' 
                        + '\n----------------\n' 
                        + e.pgerror 
                        + '----------------')
                    connection.rollback()
                    pass
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
            print('Database "' + name + '" does not exists')
            exit(1)
        
        port = rw.get_value(sectionName, 'port', '')
        # default PgSQL port, if not specified directly
        if not port:
            port = '5432'
        
        username = rw.get_value(sectionName, 'user', '')
        if len(username) == 0:
            username = input('username:')
        
        password = rw.get_value(sectionName, 'password', '')
        if len(password) == 0:
            password = getpass.getpass()
        
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
            if os.path.exists(connection + '/structure/' + s):
                print("Schema '" + s + "' in '" + connection + "' already exists")
            else :
                os.makedirs(connection + '/structure/' + s)
        return

    def createTableStructure(self, conn, schema):
        tables = self.getTables(conn, schema)
        path = "%s/structure/%s/tables" % (conn, schema)
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
    
    def addNewFilesToPatch(self, directory):
        r = git.Repo()
        currentCommit = r.commit(r.active_branch.name)
        remoteCommit = r.commit(self.patchTarget)
        diffIndex = remoteCommit.diff(currentCommit)

        for newItem in diffIndex.iter_change_type('A'):
            if re.match('^([^\/]*\/){3}' + directory + '\/.*', newItem.b_path):
                db = newItem.b_path.split('/')[0]
                if db in self.patchData.keys():
                    self.patchData[db]['new'].append({
                        'file': newItem.b_path,
                        'content': self.getFileContent(newItem.b_path)
                    })
                else:
                    self.patchData[db] = {
                        'new': [],
                        'update': [],
                        'delete': []
                    }
                        
                    self.patchData[db]['new'].append({
                        'file': newItem.b_path,
                        'content': self.getFileContent(newItem.b_path)
                    })
    
    def getFileContent(self, filePath):
        s = ''
        with open(filePath, 'r') as f:
            s = f.read()
        return s
    
    def pushChangesToPatchFile(self, next):
        patchPath = self.getPatchName(next)
        for db in self.patchData.keys():
            patchName = patchPath.split('/')[-1]
            self.registerPatch(patchName, db)

            if self.checkPatchDataDb(db):
                mode = 'w'
                fileName = patchPath + '/' + db + '.sql'
                if os.path.exists(fileName):
                    mode = 'a'

                with open(fileName, mode) as f:
                    for changeType in ['delete', 'new', 'update']:
                        isFirst = True
                        for dataDict in self.patchData[db][changeType]:
                            if isFirst:
                                isFirst = False
                            else:
                                f.write('\n\n')
                            f.write('-- ' + dataDict['file'] + '\n')
                            f.write(re.sub('\n\n+', '\n\n', dataDict['content']))
        
        self.resetPatchData()
    
    def addAlteredFilesToPatch(self, directory):
        r = git.Repo()
        currentCommit = r.commit(r.active_branch.name)
        remoteCommit = r.commit(self.patchTarget)
        diffIndex = remoteCommit.diff(currentCommit)

        for newItem in diffIndex.iter_change_type('M'):
            if directory is 'tables':
                if newItem.b_path != newItem.a_path:
                    db = newItem.b_path.split('/')[0]
                    if db not in self.patchData:
                        self.patchData[db] = {
                            'new': [],
                            'update': [],
                            'delete': []
                        }
                    self.patchData[db]['new'].append({
                        'file': newItem.b_path,
                        'content': self.getFileContent(newItem.b_path)
                    })
                    print ('testing ', newItem.b_path, newItem.a_path)
                    continue
                else:
                    print ('testing2 ', newItem.b_path, newItem.a_path)
                addToPatch = self.checkTableDiff(newItem, newItem.b_path)
                if addToPatch:
                    db = newItem.b_path.split('/')[0]
                    if db in self.patchData:
                        self.patchData[db]['update'].append({
                            'file': newItem.b_path,
                            'content': addToPatch
                        })
                    else:
                        self.patchData[db] = {
                            'new': [],
                            'update': [],
                            'delete': []
                        }

                        self.patchData[db]['update'].append({
                            'file': newItem.b_path,
                            'content': addToPatch
                        })
            # else:
                # self.patchData['update'][newItem.b_path] = self.getFileContent(newItem.b_path)
        return
    
    def addDeletedFilesToPatch(self, directory):
        r = git.Repo()
        currentCommit = r.commit(r.active_branch.name)
        remoteCommit = r.commit(self.patchTarget)
        diffIndex = remoteCommit.diff(currentCommit)

        for removedItem in diffIndex.iter_change_type('D'):
            pathArray = removedItem.a_path.split('/')
            if len(pathArray) > 2 \
                and pathArray[2] == directory \
                and directory == 'tables':

                tableName = pathArray[-3] + '\.' \
                    + pathArray[-1].split('.')[0]
                db = pathArray[0]
                if db in self.patchData:
                    self.patchData[db]['delete'].append({
                        'file': removedItem.a_path,
                        'content': 'DROP TABLE IF EXISTS ' + tableName.replace('\.', '.') + ';\n\n'
                    })
        return
    
    def checkTableDiff(self, itemBlob, filePath):
        targetFile = itemBlob.a_blob.data_stream.read().decode('utf-8')
        currentFile = itemBlob.b_blob.data_stream.read().decode('utf-8')
        tableName = filePath.split('/')[-3] + '\.' + filePath.split('/')[-1].split('.')[0]

        # filter out comments
        currentFileParts = currentFile.split('\n')
        currentFileParts = [row for row in currentFileParts if not re.match('^\h*--', row)]
        currentFileParts = '\n'.join(currentFileParts)

        targetFileParts = targetFile.split('\n')
        targetFileParts = [row for row in targetFileParts if not re.match('^\h*--', row)]
        targetFileParts = '\n'.join(targetFileParts)

        # split both files per-command
        currentFileParts = currentFileParts.split(';')
        targetFileParts = targetFileParts.split(';')

        # targetFileParts = [row for row in targetFileParts if not re.match('^\h*--', row)]
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
        remainingFilePartsCurrent = {}
        remainingFilePartsTarget = {}

        for el in currentFileParts:
            el_clear = "".join(el.lower().split('\n'))
            reObj = re.search('create\s+table\s*' + tableName + '\s*\((.*)\)', el_clear)
            if reObj:
                createTableCurrent = reObj.group(1)
            else:
                el_clear = "".join(el_clear.split())
                remainingFilePartsCurrent[el_clear] = el

        for el in targetFileParts:
            el_clear = "".join(el.lower().split('\n'))
            reObj = re.search('create\s+table\s*' + tableName + '\s*\((.*)\)', el_clear)
            if reObj:
                createTableTarget = reObj.group(1)
            else:
                el_clear = "".join(el_clear.split())
                remainingFilePartsTarget[el_clear] = el
                # createTableTarget = ''

        # split the create table into column definitions. Strip whitespace for eaier comparison        
        createTableColumnsCurrent = createTableCurrent.split(',')
        createTableColumnsTarget = createTableTarget.split(',')
        checkTableColumnsTarget = [''.join(c.split()) for c in createTableColumnsTarget]
        checkTableColumnsCurrent = [''.join(c.split()) for c in createTableColumnsCurrent]
        isAltered = False
        
        for c in createTableColumnsTarget:
            colCheck = ''.join(c.split())
            if c is not '' and colCheck not in checkTableColumnsCurrent:
                sql += '\tDROP COLUMN IF EXISTS ' + c.lstrip().split()[0] + ',\n'
                isAltered = True
        
        for c in createTableColumnsCurrent:
            colCheck = ''.join(c.split())
            if c is not '' and colCheck not in checkTableColumnsTarget:
                sql += '\tADD COLUMN IF NOT EXISTS ' + c.lstrip() + ',\n'
                isAltered = True

        if isAltered:
            sql = sql.rstrip(',\n') + ';\n\n'
        else:
            sql = ''
        
        # whatever remains in the local file and is not identical to the target file
        # should be considered an alteration and added to patch
        isAltered = False
        remainingFilePartsTargetCheck = remainingFilePartsTarget.keys()
        for key, value in remainingFilePartsCurrent.items():
            if key is not '' and key not in remainingFilePartsTargetCheck:
                isAltered = True
                sql += value + ';\n'

        return sql
    
    def setPatchTarget(self):
        r = git.Repo()
        rw = r.config_writer()
        branch = r.active_branch.name
        sectionName = "branch \"%s\"" % branch
        self.patchTarget = None

        if not rw.has_section(sectionName) or not rw.has_option(sectionName, 'database'):
            if len(self.config['default_database']) > 0:
                rw.release()
                self.remote_add([self.config['default_database']])
                rw = r.config_writer()

        if rw.has_section(sectionName) and rw.has_option(sectionName, 'database'):
            self.patchTarget = rw.get(sectionName, 'database')
            rw.release()
        else:
            print("Branch '%s' is not tracking any database" % branch)
            rw.release()
            exit(1)

    def getSimplePatchNumber(self, current, next=True):
        if next:
            return int(current) + 1
        return int(current)

    def getPatchName(self,next=True):
        r = git.Repo()
        rw = r.config_writer()
        branch = r.active_branch.name
        sectionName = "branch \"%s\"" % branch
        numberingMethod = None
        currentNumber = None
        if rw.has_section(sectionName) \
            and rw.has_option(sectionName, 'numbering') \
            and rw.has_option(sectionName, 'current'):
            
            numberingMethod = rw.get(sectionName, 'numbering')
            currentNumber = rw.get(sectionName, 'current')
        
        if numberingMethod is None or currentNumber is None:
            print("Patch numbering method was not selected")
        
        number = None
        if numberingMethod == 'simple':
            number = self.getSimplePatchNumber(currentNumber, next)
        
        if number is None:
            print('Patch number could not be calculated')
            exit(1)

        if not os.path.exists('patches'):
            os.mkdir('patches')

        rw.set_value(sectionName, 'current', number)

        filePath = 'patches/patch_%s' % number
        if not os.path.exists(filePath):
            os.mkdir(filePath)
        
        rw.release()
        return filePath
    
    def checkPatchData(self):
        for db in self.databases:
            for key in ['new', 'delete', 'update']:
                if len(self.patchData[db][key]) > 0:
                    return True
        return False
    
    def checkPatchDataDb(self, dbName):
        for key in ['new', 'delete', 'update']:
            if len(self.patchData[dbName][key]) > 0:
                return True
        return False

    def checkGitDbInitialized(self, dbName):
        if dbName in self.connections.keys():
            connection = self.connections[dbName]
            cursor = connection.cursor()
            cursor.execute('''SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = 'git_db' ''')
            
            record = cursor.fetchone()
            if record is None:
                self.createGitDbSchema(dbName)
            
            return True
        print('[WARNING] database \'%s\' did not initialize git-db tables correctly' % dbName)
        return False

    def createGitDbSchema(self, name):
        connection = self.connections[name]
 
        print('creating git_db schema in database \'%s\'' % name)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS git_db;")

        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS git_db.query (
                id SERIAL NOT NULL,
                name VARCHAR(128) NOT NULL,
                namespace VARCHAR(128) NOT NULL,
                path VARCHAR(256) NOT NULL,
                timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                applied BOOLEAN DEFAULT FALSE,
                applied_timestamp timestamp,
                applied_patch_id INT
            );

            CREATE TABLE IF NOT EXISTS git_db.patch (
                id SERIAL NOT NULL,
                name VARCHAR(128) NOT NULL,
                timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                applied BOOLEAN DEFAULT FALSE,
                applied_timestamp timestamp
            );
        ''')
        connection.commit()

    def replaceWildcards(self, name):
        r = git.Repo()
        branch = r.active_branch.name

        timestamp = str(int(time()))

        branch = branch.replace('/', '_')
        name = name.replace('{branch}', branch)
        name = name.replace('{timestamp}', timestamp)
        return name, timestamp
    
    def addQueriesToPatch(self):
        patchPath = self.getPatchName(False)
        for dbName, connection in self.connections.items():
            self.registerPatch(patchName, db)
            patchName = patchPath.split('/')[-1]
            queryFiles, fileIds = self.getQueryFilesForPatch(connection, patchName)
            for f in queryFiles:
                self.patchData[dbName]['new'].append({
                    'file': f,
                    'content': self.getFileContent(f)
                })
            self.registerQueryFilesInPatch(dbName, fileIds)
        return

    def getCurrentDb(self, databaseName):
        url, port, username, password = self.getDatabaseConnectionInfo(self.getDatabaseFromPatchTarget())
        connection = self.connect(url, port, username, password, databaseName)
        cursor = connection.cursor()
        return connection, cursor

    def registerQuery(self, context):
        connection, cursor = self.getCurrentDb(context['database'])

        contextSet = set(context['record'])
        recordSet = set(['name', 'namespace', 'path', 'timestamp', 'applied', 'applied_timestamp', 'applied_patch_id'])
        recordSet = recordSet.intersection(contextSet)

        query = "INSERT INTO git_db.query ("
        params = []
        for column in recordSet:
            query += column + ', '
            params.append(context['record'][column])
        query = query.rstrip(', ')
        query += ') VALUES ('
        for i in range(len(recordSet)):
            query += '%s, '
        query = query.rstrip(', ')
        query += ') RETURNING id;'

        cursor.execute(query % tuple(params))
        record = cursor.fetchone()
        cursor.close()
        connection.commit()
        print ('query file registered in the database \'' + self.getDatabaseFromPatchTarget() \
            + '.' + context['database'] + '.git_db.query\', under id=' + str(record[0]))
        return

    def getDatabaseFromPatchTarget(self):
        dbName = self.patchTarget
        dbName.lstrip(self.config['database_branch_prefix'] + '/')
        return dbName.lstrip(self.config['database_branch_prefix'] + '/')
        
    def resetPatchData(self):
        self.patchData = {}
        for db in self.databases:
            self.patchData[db] = {
                'new': [],
                'delete': [],
                'update': []
            }
        return
    
    def deletePatchFiles(self):
        patchPath = self.getPatchName(False)
        shutil.rmtree(patchPath)
        return

    def getQueryFilesForPatch(self, connection, patchName):
        cursor = connection.cursor()
        cursor.execute('''SELECT q.path, q.id
            FROM git_db.query q
            LEFT JOIN git_db.patch p ON p.id = q.applied_patch_id
            WHERE (p.id IS NULL OR p.id = %d) AND q.applied = FALSE
            ORDER BY q.timestamp ASC;''' % self.patchId)
        records = cursor.fetchall()
        return [r[0] for r in records], [r[1] for r in records]
    
    def registerPatch(self, patchName, dbName):
        if (self.checkGitDbInitialized(dbName)):
            connection = self.connections[dbName]
            cursor = connection.cursor()
            cursor.execute('''SELECT id 
                FROM git_db.patch 
                WHERE name = '%s' ''' % patchName)
            record = cursor.fetchone()

            if record is None:
                print('registering patch \'%s\' for database \'%s\'', patchName, dbName)
                cursor.execute('INSERT INTO git_db.patch (name) VALUES(\'%s\') RETURNING id' % patchName)
                record = cursor.fetchone()

            self.patchId = record[0]
            connection.commit()    
            return True
        
        return False
    
    def registerQueryFilesInPatch(self, dbName, queryRecordIds):
        if (len(queryRecordIds) == 0):
            return
        connection = self.connections[dbName]
        cursor = connection.cursor()
        print(queryRecordIds, self.patchId)
        cursor.execute('UPDATE git_db.query SET applied_patch_id = %d WHERE id IN (%s)' 
            % (self.patchId, ','.join([str(q) for q in queryRecordIds])) )

    def registerExistingFiles(self, patchName, dbName):
        for (dirpath, dirnames, filenames) in os.walk(dbName + '/queries'):
            for f in filenames:
                path = '/'.join(dirpath.split('/'))[2:]
                timestamp = str(int(time()))
        
                context = {
                    'database': dbName,
                    'record': {
                        'name': '\'' + f + '\'',
                        'path': '\'' + dirpath + '\'',
                        'timestamp': 'to_timestamp(' + timestamp + ')',
                        'namespace': '\'' + path + '\''
                    }
                }
                self.registerQuery(context)
                print("new query file was registered: '" + f + "'")