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
The idea behind git-db is to make incremental changes that are easy to track and diff. Open any *.sql file, let's say a table on my database contains this create table definition:
```sql
CREATE TABLE auth."user" (
    id integer NOT NULL,
    username character varying(64) NOT NULL,
    create_time datetime NOT NULL
);
```
and I want to add another column there. To do it I just alter the create table statement like so:
```sql
CREATE TABLE auth."user" (
    id integer NOT NULL,
    username character varying(64) NOT NULL,
    create_time datetime NOT NULL,
    first_name character verying(128)
);
```
I commit this changes:
```bash
git add .
git commit -m 'add a column'
git db remote add local
```
and let git db script now this branch, by default, always refers to the "local" database when any database-related command is used:
```bash
git db remote add local
```