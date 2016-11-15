package polyjug_test

import (
    "os"
    "testing"
    "github.com/wambosa/expect"
    "github.com/wambosa/polyjug"
)

func Test_GIVEN_that_query_is_valid_WHEN_Exec_is_called_THEN_expect_database_to_be_created(t *testing.T) {
    
    query := "CREATE TABLE planets (id integer not null primary key, name text)"
    
    jug := polyjug.Jug {
        DriverName: "sqlite3",
        Path: "e.db3",
    }
    
    _, err := jug.Exec(query)
    
    if err != nil {t.Error(err)}
    
    expecting := expect.TestCase {
        T: t,
        Value: os.Remove("e.db3"),
    }
    
    expecting.Falsy()
}

func Test_GIVEN_that_database_table_is_populated_WHEN_Query_is_called_THEN_expect_map_of_records(t *testing.T) {
  
    jug := polyjug.Jug {
        DriverName: "sqlite3",
        Path: "q.db3",
    }
    
    init := "CREATE TABLE planets (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name text);"
    insert := "INSERT INTO planets (name) VALUES ('Vegeta'), ('Earth'), ('Namek');"
    
    _, err := jug.Exec(init)
    if err != nil {t.Error(err)}
    
    _, err = jug.Exec(insert)
    if err != nil {t.Error(err)}
    
    res, err := jug.Query("SELECT * FROM planets")
    
    if err != nil {t.Error(err)}

    expecting := expect.TestCase {
        T: t,
        Value: res,
    }
    
    os.Remove("q.db3")
     
    expecting.Truthy()
}

func Test_GIVEN_that_the_column_type_is_string_WHEN_Query_returns_byte_array_THEN_expect_cast_from_interface_success(t *testing.T) {
  
    jug := polyjug.Jug {
        DriverName: "sqlite3",
        Path: "v.db3",
    }
    
    init := "CREATE TABLE planets (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name text);"
    insert := "INSERT INTO planets (name) VALUES ('Vegeta'), ('Earth'), ('Namek');"
    
    _, err := jug.Exec(init)
    if err != nil {t.Error(err)}
    
    _, err = jug.Exec(insert)
    if err != nil {t.Error(err)}
    
    res, err := jug.Query("SELECT * FROM planets")
    
    if err != nil {t.Error(err)}
    
    expecting := expect.TestCase {
        T: t,
        Value: string( res[2]["name"].([]byte) ),
    }
    
    os.Remove("v.db3")
    
    expecting.ToBe("Namek")
}