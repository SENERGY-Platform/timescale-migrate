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
)

func (l *Lib) recreateCAGG() error {
	log.Println("recreating CAGG")

	query := "SELECT view_name, view_definition FROM timescaledb_information.continuous_aggregates;"
	res, err := l.sourceDb.Query(query)
	if err != nil {
		return err
	}

	for res.Next() {
		var viewName string
		var viewDefinition string
		err = res.Scan(&viewName, &viewDefinition)
		if err != nil {
			return err
		}

		_, err = l.targetDb.Exec("CREATE MATERIALIZED VIEW IF NOT EXISTS \"" + viewName + "\" WITH (timescaledb.continuous) AS " + viewDefinition)
		if err != nil {
			return err
		}
		log.Println(viewName)
	}

	log.Println("finished migrating CAGG")
	return nil
}
