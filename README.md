#Aries Integration for MySQL Databases

This is an integration to store data in [MySQL Databases](https://www.mysql.com/).


##Configuration

###Schema
The name of the schema to insert data.
```javascript
"schema": "test_schema",
```

###Table
The name of the table.
```javascript
"table": "test_table",
```

###Drop
Set to true to drop the existing table and refill it, or false to insert as new data.
```javascript
"drop": true,
```

###JSON
Set to true if incoming data is JSON. False otherwise.
```javascript
"json": true,
```

###Connection
* Host: The url of the MySQL database.
* Port: The port of the database.
* User: The username, used for authentication.
* Password: The password associated with the user account.
* Database: The database where data should be stored.
```javascript
"connection" : {
    "host" : "mysql.com",
    "port" : 3306,
    "user" : "root",
    "password" : "veryinsecure",
    "database" : "test_database"
},
```

###Example Config
```javascript
{
    "schema" : "test_schema",
    "table" : "test_table",
    "drop" : true,
    "json" : true,
    "connection" : {
        "host" : "mysql.com",
        "port" : 3306,
        "user" : "root",
        "password" : "verysecure",
        "database" : "test_database"
    }
}
```