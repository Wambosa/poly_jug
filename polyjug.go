package polyjug

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

type Jug struct {
	DB         *sql.DB
	DriverName string
	Path       string
}

func New(driverName string, path string) (*Jug, error) {

	// note: used for single transactions with reconnect (good for my slow polling)
	return &Jug{
		DB:   nil,
		Path: path,
	}, nil
}

func NewPersistant(driverName string, path string) (*Jug, error) {

	// note: used when the connection should remain open between transactions

	db, err := sql.Open("sqlite3", path)

	if err != nil {
		return nil, err
	}

	return &Jug{
		DB:   db, //if this is set, then do not defer db.close
		Path: path,
	}, nil
}

func (j *Jug) Query(query string) (resultSet []map[string]interface{}, err error) {

	db, err := sql.Open(j.DriverName, j.Path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	rows, err := db.Query(query)

	if err != nil {
		return nil, err
	}

	records := make([]map[string]interface{}, 0)

	headers, _ := rows.Columns()

	defer rows.Close()

	aRecordValues := make([]interface{}, len(headers))
	aRecordValuesPtrs := make([]interface{}, len(headers))

	for rows.Next() {

		for i, _ := range headers {
			aRecordValuesPtrs[i] = &aRecordValues[i]
		}

		rows.Scan(aRecordValuesPtrs...)

		thisRecord := make(map[string]interface{})

		for i, fieldName := range headers {
			thisRecord[fieldName] = aRecordValues[i]
		}

		records = append(records, thisRecord)
	}

	return records, rows.Err()
}

func (j *Jug) Exec(query string, params ...interface{}) (sql.Result, error) {

	db, err := sql.Open(j.DriverName, j.Path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	transaction, err := db.Begin()

	if err != nil {
		return nil, err
	}

	statement, err := transaction.Prepare(query)

	if err != nil {
		return nil, err
	}

	defer statement.Close()

	res, err := statement.Exec(params...)

	if err != nil {
		return res, err
	}

	transaction.Commit()

	return res, nil
}

//Headers with column type? for later casting?
func (j *Jug) Header(tableName string) (headers map[string]string, err error) {
	return map[string]string{}, nil
}
