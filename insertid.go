package mysqlkebab

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// InsertID inserts a new record into given table and returns the last inserted id
// The 3rd param is the optional field name. If not given, the default value "id" will be used
func (l *DBLink) InsertID(table string, pairs map[string]interface{}) (int64, error) {
	if !l.supposedReady {
		return 0, fmt.Errorf("connection not properly initialized")
	}

	if len(pairs) == 0 {
		return 0, errors.New(`mysqlkebab.InsertID(undefined values)`)
	}

	var (
		fields       []string
		placeholders []string
		parameters   []interface{}
	)

	for k, v := range pairs {
		fields = append(fields, k)
		placeholders = append(placeholders, `?`)
		parameters = append(parameters, v)
	}

	sqlQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(fields, ","), strings.Join(placeholders, ","))

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(l.executionTimeoutSeconds)*time.Second)

	defer cancel()

	rs, err := l.db.ExecContext(ctx, sqlQuery, parameters...)

	if err != nil {
		l.log(`mysqlkebab.InsertID %s db.QueryRowContext has failed: "%v"`, table, err)

		return 0, err
	}

	lstid, err0 := rs.LastInsertId()

	if err0 != nil {
		return 0, err0
	}

	return lstid, nil
}
