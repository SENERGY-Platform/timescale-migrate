/*
 * Copyright 2024 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lib

import (
	"log"
	"os/exec"
	"strconv"
	"sync"
)

type tableSchemaName struct {
	schema string
	table  string
}

func (l *Lib) copyTableData() error {
	log.Println("migrating table data with " + strconv.Itoa(l.libConfig.NumWorkers) + " parallel workers")
	schemas := "('" + l.libConfig.TimescaleSchema + "'"
	for _, schema := range l.libConfig.AdditionalSchemas {
		schemas += ", '" + schema + "'"
	}
	schemas += ")"

	query := "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN " + schemas + " AND table_name NOT IN (SELECT view_name FROM timescaledb_information.continuous_aggregates);"
	res, err := l.sourceDb.Query(query)
	if err != nil {
		return err
	}

	workQueue := make(chan tableSchemaName, l.libConfig.NumWorkers)

	wg := sync.WaitGroup{}

	for i := range l.libConfig.NumWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.copyTableDataWorker(i, workQueue)
		}()
	}

	for res.Next() {
		var schema []byte
		var table []byte
		err = res.Scan(&schema, &table)
		if err != nil {
			return err
		}
		workQueue <- tableSchemaName{schema: string(schema), table: string(table)}
	}

	close(workQueue)
	wg.Wait()

	log.Println("finished migrating table data")
	return nil
}

func (l *Lib) copyTableDataWorker(id int, workQueue <-chan tableSchemaName) {
	log.Println("Starting worker " + strconv.Itoa(id))
	for tableSchemaName := range workQueue {
		table := tableSchemaName.table
		schema := tableSchemaName.schema
		tableS := "\\\"" + schema + "\\\".\\\"" + table + "\\\""

		// log.Println("Worker " + strconv.Itoa(id) + " now working on " + tableS)

		rows, err := l.targetDb.Query("SELECT * FROM \"" + schema + "\".\"" + table + "\" LIMIT 1")
		if err != nil {
			panic(err)
		}
		if rows.Next() {
			log.Println("WARNING: Skipping table " + tableS + ": Not empty. If you want to copy this data, truncate this table first!")
			rows.Close()
			continue
		}
		rows.Close()
		cmd := "psql " + l.sourcePsqlConnStr + " -c \"\\COPY (SELECT * FROM " + tableS + ") TO stdout DELIMITER ',' CSV\" | " + "psql " + l.targetPsqlConnStr + " -c \"\\COPY " + tableS + " FROM stdin CSV\""
		ex := exec.Command("sh", "-c", cmd)
		output, err := ex.Output()
		if err != nil {
			panic(err)
		}

		log.Print(tableS, string(output))
	}
}
