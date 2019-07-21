# git-db
Script to maintain a PostgreSQL database using combination of git and pg_dump. The goal is to achieve version control of a database structure that would allow to manage (and review) incremental changes in a git-like fashion.

# Dependencies

Make sure all these are installed:

* git
* python3
* pg_dump
* python3-psycopg2
* gitpython (for python3)

# Idea

## Repository file structure

A database structure is downloaded into a tree-like directory structure:

1. top-level are database directories
2. each databases directory contains two sub-directories:
    * structure - for tables, views, triggers, functions etc
    * queries - for custom SQL to be executed
3. structure directory contains schema sub-directories
4. schema directory contains tables, views, triggers and functions directories
5. each of the above contains a number of *.sql files, each corresponding to a separate database entity. For example, a file `app/structure/auth/tables/user.sql` holds a current state of a `users` table, under `auth` schema in the `app` database.

## `database/*` branches

Branches prefixes with `database/` are special branches that are used to pull down current state of the actual database a repository represents. No changes should be commited to them directly as any changes will be overwritten by a `git db database pull` command. 

# Example

## Basic repo setup

Create an empty directory and initialize a new git-db repo:
```bash
mkdir database-repo
cd database-repo
git db init
```
It simply initializes a pretty standard, empty git repo and writes a bunch of extra lines to `.git/config` with default git db settings. Next, let's add a database connection definition:
```bash
git db database add local 127.0.0.1:5432 admin admin
```
The above adds a database connection, named `local`, to the `.git/config` file. It connected to a localhost instance, using a standard 5432 port, using credentials admin/admin. You should see that under `[database "local"]` section in the `.git/congig` file. Othwerwise, nothing fancy happened yet, but you can test the connection to the database from the script, to make sure all dependencies work ok, and there is no typeo in the command above:
```bash
git db database check local
```
Expect to see the output of `SELECT version();`. If all is good so far, you can pull down your database structure into you repository:
```bash
git db database pull local
```
The script should inform you that it created, and switched to, a new database branch "database/local" and list all database elements it's creating structure files for. Each of these files was created with `pg_dump`, so it contains plain, easy to read SQL. You should not commit to this branch, as every time you use the `git db database pull local` again, all local changes will be overwritten by the current state of your "local" database at 127.0.0.1:5432. Check out a new branch to make your changes on:
```bash
git checkout -b local
```
and let git db script now this branch, by default, always refers to the "local" database when any database-related command is used:
```bash
git db remote add local
```
It should display back to you `Branch 'local' set to track database 'local'`. And now both your development `local` branch, and a database branch `database/local`, are set up.

## Commiting changes / creating database patches

The idea behind git-db is to make incremental changes that are easy to track and diff. Open any *.sql file, let's say a table on my database contains this create table definition:
```sql
CREATE TABLE auth."user" (
    id integer NOT NULL,
    username character varying(64) NOT NULL,
    create_time timestamp NOT NULL
);
```
and I want to add another column there. To do it I just alter the create table statement like so:
```sql
CREATE TABLE auth."user" (
    id integer NOT NULL,
    username character varying(64) NOT NULL,
    create_time timestamp NOT NULL,
    first_name character varying(128)
);
```
I commit these changes:
```bash
git add .
git commit -m 'add a column'
```
Now my development branch `local` defines a different database structure to my actual database, as stored in the `database/local` branch. Let's create a patch file that needs to be applied to the database to make both structures consistent:
```bash
git db patch create
```
You should see an information about a patch being created, and a path to where it's stored: `Patch created: patches/patch_1`. It is a path to a directory that stores patch files per database found on the PostgreSQL server. In my case, having a database `auth` on my test server, I see a file: `patches/patch_1/auth.sql`, that contains:
```sql
-- auth/structure/auth/tables/user.sql
ALTER TABLE auth.user
        ADD COLUMN IF NOT EXISTS first_name character verying(128);
```
where there commented part shows the source file responsible for the change. In this case a change in `user.sql` trigger a corresponding `ALTER TABLE (...)` to be added to the patch. 
> You might also see informations messages like these:
> ```bash
> [INFO] creating git_db schema in database 'auth'
> [INFO] registering patch 'patch_1' for database 'auth'
> ```
> or warnings like these:
> ```bash
> [WARNING] database 'test' did not initialize git-db tables correctly
> [WARNING] cannot register patch 'patch_1' for database 'test'
> [WARNING] database 'test' did not initialize git-db tables correctly
> [WARNING] cannot register patch 'patch_1' for database 'test'
> ```
> Git-db creates a `git_db` schema in each database to track what has been already applied to a database and what has not. If you see warnings informing you that these tables were not initializes correctly it's most likely because you've added a table to a database that does not yet exist, therefore patches cannot be registered when they are created (still, they will get registered when a patch is applied).

If you are happy with the patch file created, you can apply it to you database:
```bash
git db patch apply
```
As this is a fairly dangerous step (after all you modify your database structure, potentially in a production environment), extra information is displayed back to you, informing you which patch is going to be applied to which of the defined database connections: `Do you want to apply patch 'patch_1' to the database 'local'? [y/n]`. 
Expect to see this:
```bash
[INFO] Applying patch file 'patches/patch_1/test.sql'
[INFO]...ok
```
Now, before doing any more changes to the database structure, make sure you pull down your database branch and merge it back into your development branch:
```bash
git db database pull local
git checkout local
git merge database local
```

# TODOs

1. so far git-db just supporst tables (needs to support views, triggers, functions etc)