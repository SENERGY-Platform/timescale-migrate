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
	"encoding/json"
	"log"

	"github.com/jackc/pgtype"
)

type CaggPolicyConfig struct {
	EndOffset   *string `json:"end_offset"`
	StartOffset *string `json:"start_offset"`
}

type RetentionPolicyConfig struct {
	DropAfter string `json:"drop_after"`
}

func (l *Lib) migratePolicies() (err error) {
	log.Println("start migrating policies")
	// CAGG refresh policies
	rows, err := l.sourceDb.Query("SELECT schedule_interval, config, view_name FROM timescaledb_information.jobs, timescaledb_information.continuous_aggregates where continuous_aggregates.materialization_hypertable_name = jobs.hypertable_name ;")
	if err != nil {
		return err
	}

	for rows.Next() {
		var schedule_interval pgtype.Interval
		var schedule_interval_str, config, view_name string
		err = rows.Scan(&schedule_interval, &config, &view_name)
		if err != nil {
			return err
		}
		tmp, err := schedule_interval.EncodeText(nil, []byte(schedule_interval_str))
		if err != nil {
			return err
		}
		schedule_interval_str = string(tmp)
		var policyConfig CaggPolicyConfig
		err = json.Unmarshal([]byte(config), &policyConfig)
		if err != nil {
			return err
		}
		var startOffset, endOffset string
		if policyConfig.StartOffset != nil {
			startOffset = "INTERVAL '" + *policyConfig.StartOffset + "'"
		} else {
			startOffset = "NULL"
		}
		if policyConfig.EndOffset != nil {
			endOffset = "INTERVAL '" + *policyConfig.EndOffset + "'"
		} else {
			endOffset = "NULL"
		}
		query := "SELECT add_continuous_aggregate_policy('\"" + view_name + "\"', start_offset => " + startOffset + ", end_offset => " + endOffset + ", schedule_interval => INTERVAL '" + schedule_interval_str + "', if_not_exists => TRUE);"
		_, err = l.targetDb.Exec(query)
		if err != nil {
			if err.Error() == "ERROR: could not find end_offset in config for job (SQLSTATE XX000)" {
				// This happens if policy exists for some reason!?
				log.Println("WARN:", view_name, err.Error())
				continue
			}
			return err
		}
		log.Println(view_name)
	}

	// retention policies
	rows, err = l.sourceDb.Query("SELECT schedule_interval, config, hypertable_name FROM timescaledb_information.jobs where proc_name = 'policy_retention';")
	if err != nil {
		return err
	}
	for rows.Next() {
		var schedule_interval pgtype.Interval
		var schedule_interval_str, config, hypertable_name string
		var policyConfig RetentionPolicyConfig
		err = rows.Scan(&schedule_interval, &config, &hypertable_name)
		if err != nil {
			return err
		}
		tmp, err := schedule_interval.EncodeText(nil, []byte(schedule_interval_str))
		if err != nil {
			return err
		}
		schedule_interval_str = string(tmp)
		err = json.Unmarshal([]byte(config), &policyConfig)
		if err != nil {
			return err
		}
		_, err = l.targetDb.Exec("SELECT add_retention_policy('\"" + hypertable_name + "\"', drop_after => INTERVAL '" + policyConfig.DropAfter + "', schedule_interval => '" + schedule_interval_str + "', if_not_exists => TRUE);")
		if err != nil {
			return err
		}
		log.Println(hypertable_name)
	}

	log.Println("finsihed migrating policies")

	return nil
}
