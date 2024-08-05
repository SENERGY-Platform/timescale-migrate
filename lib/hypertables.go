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
	"fmt"
	"log"
)

func (l *Lib) createHypertables() error {
	log.Println("creating hypertables")

	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + l.libConfig.TimescaleSchema + "';"
	res, err := l.targetDb.Query(query)
	if err != nil {
		return err
	}
	for res.Next() {
		var table []byte
		err = res.Scan(&table)
		if err != nil {
			return err
		}
		query := "SELECT create_hypertable('\"" + l.libConfig.TimescaleSchema + "\".\"" + string(table) + "\"', '" + l.libConfig.TimeFieldName + "', if_not_exists => TRUE);"
		_, err := l.targetDb.Exec(query)
		if err != nil {
			return err
		}
		fmt.Print(".")
	}
	return nil
}
